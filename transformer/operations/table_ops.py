import os
import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
import sqlglot
from sqlglot import expressions as exp
from ..utils.parsers import SqlParser

class TableOperations:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.parser = SqlParser()

    async def create_table(self, sql_statement: str, current_database: str = 'csvgres') -> None:
        """Create a new table from CREATE TABLE statement"""
        try:
            # if current_database is None:
            #     raise ValueError('Not connected to any database. Use connect command first.')

            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Create) or parsed.args.get('kind') != 'TABLE':
                raise ValueError('Invalid CREATE TABLE statement')

            table_name = parsed.args['this'].this.this
            database_path = os.path.join(self.base_dir, current_database)
            metadata_path = os.path.join(database_path, '.metadata')
            data_path = os.path.join(database_path, 'tables')

            os.makedirs(metadata_path, exist_ok=True)
            os.makedirs(data_path, exist_ok=True)

            file_path = os.path.join(data_path, f'{table_name}.csv')
            meta_path = os.path.join(metadata_path, f'{table_name}.json')
            
            if os.path.exists(file_path):
                raise ValueError(f'Table {table_name} already exists')
            
            columns = self.parser.extract_columns(parsed)

            metadata = {
                'columns': {}
            }
            
            for col in columns:
                col_meta = {
                    'type': str(col.type)
                }
                
                if col.is_serial:
                    col_meta['is_serial'] = True
                    col_meta['initial_counter_value'] = col.initial_counter_value or 1
                    col_meta['auto_increment_counter'] = col.initial_counter_value or 1
                
                if col.primary_key:
                    col_meta['primary_key'] = True
                elif col.not_null:
                    col_meta['not_null'] = True
                
                if not col.primary_key and col.unique:
                    col_meta['unique'] = True
                    
                if col.default is not None and not col.is_serial:
                    col_meta['default'] = str(col.default) if hasattr(col.default, 'sql') else col.default
                
                if str(col.type) == 'ARRAY' and hasattr(col, 'array_subtype'):
                    col_meta['array_type'] = col.array_subtype
                    if col.default is None:
                        col_meta['default'] = []
                
                metadata['columns'][col.name] = col_meta
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                df = pd.DataFrame(columns=[col.name for col in columns])
                await asyncio.gather(
                    loop.run_in_executor(pool, lambda: df.to_csv(file_path, index=False)),
                    loop.run_in_executor(pool, lambda: self._save_metadata(meta_path, metadata))
                )
        
        except Exception as error:
            print(f'Error creating table: {error}')
            raise

    async def drop_table(self, sql_statement: str, current_database: str) -> None:
        """Drop a table from a database"""
        try:
            # if current_database is None:
            #     raise ValueError('Not connected to any database. Use connect command first.')

            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Drop) or parsed.args.get('kind') != 'TABLE':
                raise ValueError('Invalid DROP TABLE statement')

            table_name = parsed.args['this'].this.this
            data_path = os.path.join(self.base_dir, current_database, 'tables')
            metadata_path = os.path.join(self.base_dir, current_database, '.metadata')

            file_path = os.path.join(data_path, f'{table_name}.csv')
            meta_path = os.path.join(metadata_path, f'{table_name}.json')

            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                await asyncio.gather(
                    loop.run_in_executor(pool, os.remove, file_path),
                    loop.run_in_executor(pool, os.remove, meta_path) if os.path.exists(meta_path) else None
                )
            
        except Exception as error:
            print(f'Error dropping table: {error}')
            raise

    def _save_metadata(self, meta_path: str, metadata: dict) -> None:
        """Save metadata to JSON file"""
        import json
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2) 