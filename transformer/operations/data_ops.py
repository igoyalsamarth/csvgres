import os
import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
import sqlglot
from sqlglot import expressions as exp
from ..utils.parsers import SqlParser
from ..utils.type_handlers import TypeHandler

class DataOperations:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.parser = SqlParser()
        self.type_handler = TypeHandler()

    async def insert(self, sql_statement: str, current_database: str) -> None:
        """Insert data into table from INSERT statement"""
        try:
            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Insert):
                raise ValueError('Invalid INSERT statement')

            table_name = parsed.args['this'].this.this
            database_path = os.path.join(self.base_dir, current_database)
            metadata_path = os.path.join(database_path, '.metadata')
            data_path = os.path.join(database_path, 'tables')
            
            file_path = os.path.join(data_path, f'{table_name}.csv')
            meta_path = os.path.join(metadata_path, f'{table_name}.json')
            
            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')

            # Load metadata
            metadata = await self._load_metadata(meta_path)
            columns_meta = metadata['columns']

            # Create DataFrame from VALUES
            values_data = []
            if isinstance(parsed.args['expression'], exp.Values):
                for tuple_expr in parsed.args['expression'].expressions:
                    row = [val.this if isinstance(val, exp.Literal) else val.this.this 
                          for val in tuple_expr.expressions]
                    values_data.append(row)
            # Get the columns from the INSERT statement or use all columns
            if parsed.args['this'].expressions:
                insert_columns = [col.this for col in parsed.args['this'].expressions]
                for col in insert_columns:
                    if col not in columns_meta:
                        raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")
            else:
                insert_columns = list(columns_meta.keys())

            df_dict = {col: [] for col in columns_meta.keys()}  # Initialize all columns

            for row in values_data:
                row_dict = {col: None for col in columns_meta.keys()}  # Default None for all columns

                # Map each column in insert_columns to its corresponding value in row
                for i, col in enumerate(insert_columns):
                    if i < len(row):  # Ensure we do not go out of index
                        row_dict[col] = row[i]  

                # Append mapped values correctly
                for col in df_dict:
                    df_dict[col].append(row_dict[col])


            new_rows = pd.DataFrame(df_dict)

            for col_name, col_meta in columns_meta.items():
                if col_meta.get('is_serial', False):
                    null_mask = new_rows[col_name].isnull()
                    new_rows.loc[null_mask, col_name] = col_meta['auto_increment_counter']
                    col_meta['auto_increment_counter'] += null_mask.sum()
                elif 'default' in col_meta:
                    default_value = self.type_handler.parse_value_with_type(
                        col_meta['default'], 
                        col_meta['type']
                    )
                    if col_meta['type'] == 'ARRAY' and isinstance(default_value, list):
                        new_rows[col_name] = new_rows[col_name].apply(lambda x: default_value if pd.isna(x) else x)
                    else:
                        new_rows[col_name] = new_rows[col_name].fillna(default_value)

            # Type validation
            for col_name, col_meta in columns_meta.items():
                if col_name in new_rows.columns:
                    # Convert values according to their types
                    try:
                        new_rows[col_name] = new_rows[col_name].apply(
                            lambda x: self.type_handler.parse_value_with_type(x, col_meta['type'])
                        )
                    except Exception as e:
                        raise ValueError(f"Type validation failed for column '{col_name}': {str(e)}")

            # Constraint validation
            for col_name, col_meta in columns_meta.items():
                # NOT NULL constraint
                if col_meta.get('not_null', False) or col_meta.get('primary_key', False):
                    if new_rows[col_name].isnull().any():
                        raise ValueError(f"Column '{col_name}' cannot be NULL")

            # Read existing data and combine
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                existing_df = await loop.run_in_executor(pool, pd.read_csv, file_path)
                combined_df = pd.concat([existing_df, new_rows], ignore_index=True)
                
                # Unique and Primary Key constraints
                for col_name, col_meta in columns_meta.items():
                    if col_meta.get('unique', False) or col_meta.get('primary_key', False):
                        duplicates = combined_df[col_name].duplicated()
                        if duplicates.any():
                            raise ValueError(f"Duplicate value in {'primary key' if col_meta.get('primary_key', False) else 'unique'} column '{col_name}'")
                
                await asyncio.gather(
                    loop.run_in_executor(pool, lambda: combined_df.to_csv(file_path, index=False)),
                    loop.run_in_executor(pool, lambda: self._save_metadata(meta_path, metadata))
                )

        except Exception as error:
            print(f'Error inserting data: {error}')
            raise

    async def select(self, sql_statement: str, current_database: str) -> pd.DataFrame:
        """Execute SELECT statement and return results"""
        try:
            parsed = sqlglot.parse_one(sql_statement)

            if not isinstance(parsed, exp.Select):
                raise ValueError('Invalid SELECT statement')

            from_expr = parsed.args['from']
            table_name = from_expr[0].this.this
            file_path = os.path.join(self.base_dir, current_database, 'tables', f'{table_name}.csv')

            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                df = await loop.run_in_executor(pool, pd.read_csv, file_path)  
                if parsed.args.get('where'):
                    where_expr = parsed.args['where']
                    condition = self.parser.parse_where_expression(where_expr)
                    df = df.query(condition, engine='python')
            result_df = df.copy() if isinstance(parsed.expressions[0], exp.Star) else df[[
                expr.this.this if isinstance(expr, exp.Column) 
                else expr.alias_or_name for expr in parsed.expressions
            ]].copy()
            
            # Convert all numeric columns to objects to handle NaN
            for col in result_df.select_dtypes(include=['float64', 'int64']).columns:
                result_df = result_df.astype({col: 'object'})  # Convert using astype with dictionary
                result_df.loc[:, col] = result_df[col].where(result_df[col].notna(), None)
            
            # Convert string columns
            for col in result_df.select_dtypes(include=['object']).columns:
                result_df.loc[:, col] = result_df[col].where(result_df[col].notna(), None)
            
            # Convert DataFrame to list of dictionaries with proper structure
            result = result_df.to_dict(orient='records')
            return result

        except Exception as error:
            print(f'Error selecting data: {error}')
            raise

    async def update_row(self, sql_statement: str, current_database: str) -> None:
        """Update data in table from UPDATE statement"""
        try:
            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Update):
                raise ValueError('Invalid UPDATE statement')
            
            table_name = parsed.args['this'].this.this
            file_path = os.path.join(self.base_dir, current_database, 'tables', f'{table_name}.csv')
            meta_path = os.path.join(self.base_dir, current_database, '.metadata', f'{table_name}.json')

            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')
            
            # Load metadata for type checking
            metadata = await self._load_metadata(meta_path)
            columns_meta = metadata['columns']
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                df = await loop.run_in_executor(pool, pd.read_csv, file_path)
                
                # Create update mask
                if parsed.args.get('where'):
                    condition = self.parser.parse_where_expression(parsed.args['where'])
                    mask = df.eval(condition)
                else:
                    mask = pd.Series([True] * len(df))
                
                # Apply updates for each SET expression
                for expr in parsed.args['expressions']:
                    col_name = expr.this.this.this
                    
                    if isinstance(expr.expression, exp.DPipe):  # Handle concatenation (||)
                        if columns_meta[col_name]['type'] == 'ARRAY':
                            new_val = expr.expression.expression.this.strip('{}')
                            
                            def update_array(x):
                                try:
                                    # Convert string representation to actual array
                                    current_array = eval(x) if pd.notna(x) and x != '[]' else []
                                    if not isinstance(current_array, list):
                                        current_array = []
                                    # Add new value to array
                                    if new_val not in current_array:
                                        current_array.append(new_val)
                                    return str(current_array)
                                except:
                                    return f'["{new_val}"]'
                            
                            df.loc[mask, col_name] = df.loc[mask, col_name].apply(update_array)
                    elif isinstance(expr.expression, exp.Sub):  # Handle subtraction (-)
                        if columns_meta[col_name]['type'] == 'ARRAY':
                            value_to_remove = expr.expression.expression.this.strip('{}')
                            
                            def remove_from_array(x):
                                try:
                                    current_array = eval(x) if pd.notna(x) and x != '[]' else []
                                    if not isinstance(current_array, list):
                                        current_array = []
                                    # Remove value from array if it exists
                                    if value_to_remove in current_array:
                                        current_array.remove(value_to_remove)
                                    return str(current_array)
                                except:
                                    return '[]'
                            
                            df.loc[mask, col_name] = df.loc[mask, col_name].apply(remove_from_array)
                    else:  # Handle normal updates
                        new_val = expr.expression.this
                        df.loc[mask, col_name] = new_val
                
                await loop.run_in_executor(pool, lambda: df.to_csv(file_path, index=False))

        except Exception as error:
            print(f'Error updating data: {error}')
            raise

    async def delete_row(self, sql_statement: str, current_database: str) -> None:
        """Delete a row from a table"""
        try:
            # if current_database is None:
            #     raise ValueError('Not connected to any database. Use connect command first.')

            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Delete):
                raise ValueError('Invalid DELETE statement')

            table_name = parsed.args['this'].this.this
            file_path = os.path.join(self.base_dir, current_database, 'tables', f'{table_name}.csv')

            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                df = await loop.run_in_executor(pool, pd.read_csv, file_path)

                if parsed.args.get('where'):
                    condition = self.parser.parse_where_expression(parsed.args['where'])
                    mask = ~df.eval(condition)
                    df = df[mask]
                else:
                    df = pd.DataFrame(columns=df.columns)
                
                await loop.run_in_executor(pool, lambda: df.to_csv(file_path, index=False))

        except Exception as error:
            print(f'Error deleting row: {error}')
            raise

    async def _load_metadata(self, meta_path: str) -> dict:
        """Load metadata from JSON file"""
        import json
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            return await loop.run_in_executor(pool, lambda: json.load(open(meta_path, 'r')))

    def _save_metadata(self, meta_path: str, metadata: dict) -> None:
        """Save metadata to JSON file"""
        import json
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2) 