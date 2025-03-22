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

            # Initialize df_dict only with columns being inserted
            df_dict = {col: [] for col in insert_columns}

            # Map values to their columns
            for row in values_data:
                for i, col in enumerate(insert_columns):
                    if i < len(row):
                        df_dict[col].append(row[i])
                    else:
                        df_dict[col].append(None)

            new_rows = pd.DataFrame(df_dict)

            # Read existing data first to get the latest auto-increment value
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                existing_df = await loop.run_in_executor(pool, pd.read_csv, file_path)

            # Add any missing columns from the table schema
            for col in columns_meta.keys():
                if col not in new_rows.columns:
                    new_rows[col] = None

            # Handle serial/auto-increment columns
            for col_name, col_meta in columns_meta.items():
                if col_meta.get('is_serial', False):
                    null_mask = new_rows[col_name].isnull()
                    if not existing_df.empty:
                        last_id = int(existing_df[col_name].max())  # Convert to Python int
                        new_rows.loc[null_mask, col_name] = range(
                            last_id + 1,
                            last_id + 1 + null_mask.sum()
                        )
                    else:
                        new_rows.loc[null_mask, col_name] = range(
                            1,
                            1 + null_mask.sum()
                        )
                    # Update the auto_increment_counter in metadata
                    if not new_rows[col_name].empty:
                        col_meta['auto_increment_counter'] = int(new_rows[col_name].max()) + 1  # Convert to Python int

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
                    # Handle VARCHAR type with length check
                    if isinstance(col_meta['type'], str) and col_meta['type'].startswith('VARCHAR'):
                        try:
                            def validate_varchar(x):
                                if pd.isna(x):
                                    return x
                                # Convert to string if not already
                                return str(x)
                            
                            # Only check length if VARCHAR(n) is specified
                            if '(' in col_meta['type']:
                                # Extract length from VARCHAR(n)
                                length = int(col_meta['type'].strip('VARCHAR()'))
                                
                                def validate_varchar(x):
                                    if pd.isna(x):
                                        return x
                                    # Convert to string if not already
                                    val = str(x)
                                    if len(val) > length:
                                        raise ValueError(f"Value '{val}' exceeds maximum length of {length}")
                                    return val
                            
                            new_rows[col_name] = new_rows[col_name].apply(validate_varchar)
                        except Exception as e:
                            raise ValueError(f"Invalid VARCHAR value in column '{col_name}': {str(e)}")
                    # Handle DECIMAL type with precision and scale
                    elif isinstance(col_meta['type'], str) and col_meta['type'].startswith('DECIMAL'):
                        try:
                            # Extract precision and scale from DECIMAL(p) or DECIMAL(p,s)
                            params = col_meta['type'].strip('DECIMAL()').split(',')
                            precision = int(params[0])
                            scale = int(params[1]) if len(params) > 1 else 0  # Default scale to 0 if not specified
                            
                            def validate_decimal(x):
                                if pd.isna(x):
                                    return x
                                # Convert to float first to handle string inputs
                                val = float(x)
                                # Check total digits and decimal places
                                str_val = f"{abs(val):.{scale}f}"
                                int_part, dec_part = str_val.split('.')
                                if len(int_part) + len(dec_part) > precision:
                                    raise ValueError(f"Value {val} exceeds precision of {precision}")
                                # Return string formatted with exact decimal places to preserve in CSV
                                return f"{val:.{scale}f}"
                            
                            new_rows[col_name] = new_rows[col_name].apply(validate_decimal)
                        except Exception as e:
                            raise ValueError(f"Invalid DECIMAL value in column '{col_name}': {str(e)}")
                    else:
                        # Existing type validation for other types
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
                
                # Save data and metadata properly with await
                await loop.run_in_executor(pool, lambda: combined_df.to_csv(file_path, index=False))
                await self._save_metadata(meta_path, metadata)

        except Exception as error:
            print(f'Error inserting data: {error}')
            raise

    async def _handle_join(self, join, df, current_database, executor, loop):
        """Handle a single JOIN operation"""
        try:
            # Get join table name without alias
            join_table = join.this.this.this if isinstance(join.this.this, exp.Table) else join.this.this.this
            join_file_path = os.path.join(self.base_dir, current_database, 'tables', f'{join_table}.csv')
            
            if not os.path.exists(join_file_path):
                raise ValueError(f'Table {join_table} does not exist')
            
            # Read joined table
            join_df = await loop.run_in_executor(executor, pd.read_csv, join_file_path)
            
            # Extract join condition
            if join.args.get('on'):
                on_clause = join.args['on']
                
                # Get the actual column names from the identifiers
                left_col = str(on_clause.this)  # Convert Identifier to string
                right_col = str(on_clause.expression)  # Convert Identifier to string
                
                # Strip any table aliases from column names
                left_col = left_col.split('.')[-1] if '.' in left_col else left_col
                right_col = right_col.split('.')[-1] if '.' in right_col else right_col
                
                # Perform the join
                return pd.merge(df, join_df, left_on=left_col, right_on=right_col, how='inner')
            
            return df
        except Exception as e:
            print(f"Join error: {e}")
            raise

    async def select(self, sql_statement: str, current_database: str) -> pd.DataFrame:
        """Execute SELECT statement and return results"""
        try:
            parsed = sqlglot.parse_one(sql_statement)

            if not isinstance(parsed, exp.Select):
                raise ValueError('Invalid SELECT statement')

            # Get the main table name without alias
            from_expr = parsed.args['from']
            if isinstance(from_expr[0].this, exp.Table):
                main_table = from_expr[0].this.this
            else:
                main_table = from_expr[0].this.this.this
            
            main_file_path = os.path.join(self.base_dir, current_database, 'tables', f'{main_table}.csv')
            
            if not os.path.exists(main_file_path):
                raise ValueError(f'Table {main_table} does not exist')

            executor = ThreadPoolExecutor()
            try:
                loop = asyncio.get_event_loop()
                df = await loop.run_in_executor(executor, pd.read_csv, main_file_path)

                # Handle JOINs if present
                if parsed.args.get('joins'):
                    for join in parsed.args['joins']:
                        df = await self._handle_join(join, df, current_database, executor, loop)

                # Handle WHERE clause
                if parsed.args.get('where'):
                    where_expr = parsed.args['where']
                    condition = self.parser.parse_where_expression(where_expr)
                    df = df.query(condition, engine='python')

                # Select columns
                if isinstance(parsed.expressions[0], exp.Star):
                    result_df = df.copy()
                else:
                    selected_columns = []
                    for expr in parsed.expressions:
                        if isinstance(expr, exp.Column):
                            # Handle column with table alias
                            col_name = str(expr.this).split('.')[-1]  # Get the column name part
                            selected_columns.append(col_name)
                        else:
                            selected_columns.append(expr.alias_or_name)
                    result_df = df[selected_columns].copy()
                
                # Handle NULL values while preserving original decimal precision
                for col in result_df.select_dtypes(include=['float64']).columns:
                    result_df = result_df.astype({col: 'object'})
                    result_df.loc[:, col] = result_df[col].where(result_df[col].notna(), None)
                
                # Handle other numeric types
                for col in result_df.select_dtypes(include=['int64']).columns:
                    result_df = result_df.astype({col: 'object'})
                    result_df.loc[:, col] = result_df[col].where(result_df[col].notna(), None)
                
                # Handle string columns
                for col in result_df.select_dtypes(include=['object']).columns:
                    result_df.loc[:, col] = result_df[col].where(result_df[col].notna(), None)
                
                return result_df.to_dict(orient='records')

            finally:
                executor.shutdown(wait=False)

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
        try:
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                with open(meta_path, 'r') as f:
                    content = await loop.run_in_executor(pool, f.read)
                
                content = content.strip()
                if not content:
                    return {"columns": {}}
                
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    content = content.replace('\n', ' ').replace('\r', '')
                    content = ' '.join(content.split())
                    return json.loads(content)
                    
        except Exception as e:
            return {"columns": {}}

    def _convert_to_json_serializable(self, obj):
        """Convert numpy types to Python native types"""
        import numpy as np
        if isinstance(obj, dict):
            return {k: self._convert_to_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_to_json_serializable(v) for v in obj]
        elif isinstance(obj, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        return obj

    async def _save_metadata(self, meta_path: str, metadata: dict) -> None:
        """Save metadata to JSON file"""
        import json
        temp_path = f"{meta_path}.tmp"  # Define temp_path before try block
        try:
            if not isinstance(metadata, dict):
                metadata = {"columns": {}}
            if "columns" not in metadata:
                metadata["columns"] = {}

            metadata = self._convert_to_json_serializable(metadata)

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                def safe_write():
                    with open(temp_path, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, indent=2, ensure_ascii=False)
                
                await loop.run_in_executor(pool, safe_write)
                
                with open(temp_path, 'r', encoding='utf-8') as f:
                    json.load(f)  # Verify JSON validity
                
                os.replace(temp_path, meta_path)

        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise ValueError(f"Error saving metadata: {str(e)}") 