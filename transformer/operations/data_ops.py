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
            # if current_database is None:
            #     raise ValueError('Not connected to any database. Use connect command first.')

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

            # Get column names from INSERT statement if provided
            provided_columns = []
            if parsed.args.get('columns'):
                provided_columns = [col.this.this for col in parsed.args['columns']]
                for col in provided_columns:
                    if col not in columns_meta:
                        raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")
            else:
                provided_columns = [col_name for col_name, col_meta in columns_meta.items() 
                                  if not col_meta.get('is_serial', False)]

            values_list = []
            if isinstance(parsed.args['expression'], exp.Values):
                for tuple_expr in parsed.args['expression'].expressions:
                    row_values = {}
                    
                    # Initialize all columns with defaults or NULL
                    for col_name, col_meta in columns_meta.items():
                        if col_meta.get('is_serial', False):
                            row_values[col_name] = col_meta['auto_increment_counter']
                            col_meta['auto_increment_counter'] += 1
                        elif 'default' in col_meta:
                            row_values[col_name] = self.type_handler.parse_value_with_type(
                                col_meta['default'], 
                                col_meta['type']
                            )
                        else:
                            row_values[col_name] = None

                    # Fill in provided values
                    for i, val in enumerate(tuple_expr.expressions):
                        col_name = provided_columns[i]
                        col_meta = columns_meta[col_name]
                        
                        if not col_meta.get('is_serial', False):
                            raw_value = val.this if isinstance(val, exp.Literal) else val.this.this
                            
                            if isinstance(val, exp.Literal):
                                if val.is_string and 'INT' in col_meta['type'].upper():
                                    raise ValueError(f"Invalid integer value: String literal '{raw_value}' cannot be used for INT column '{col_name}'")
                            
                            parsed_value = self.type_handler.parse_value_with_type(raw_value, col_meta['type'])
                            row_values[col_name] = parsed_value

                    # Validate constraints
                    for col_name, value in row_values.items():
                        col_meta = columns_meta[col_name]
                        if value is None and (col_meta.get('not_null', False) or col_meta.get('primary_key', False)):
                            raise ValueError(f"Column '{col_name}' cannot be NULL")

                    values_list.append(row_values)

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                df = await loop.run_in_executor(pool, pd.read_csv, file_path)
                new_rows = pd.DataFrame(values_list)
                combined_df = pd.concat([df, new_rows], ignore_index=True)
                
                # Validate unique and primary key constraints
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
            # if current_database is None:
            #     raise ValueError('Not connected to any database. Use connect command first.')

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
                    condition = self.parser.parse_where_expression(parsed.args['where'])
                    df = await loop.run_in_executor(pool, lambda: df.query(condition))

                if isinstance(parsed.expressions[0], exp.Star):
                    return df
                else:
                    columns = [expr.this.this if isinstance(expr, exp.Column) 
                              else expr.alias_or_name for expr in parsed.expressions]
                    return df[columns]

        except Exception as error:
            print(f'Error selecting data: {error}')
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