import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
import shutil
import sqlglot
from sqlglot import expressions as exp

class DatabaseOperations:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        
    async def create_database(self, sql_statement: str) -> str:
        """Create a new database from CREATE DATABASE statement"""
        try:
            parsed = sqlglot.parse_one(sql_statement)

            if not isinstance(parsed, exp.Create) or parsed.args.get('kind') != 'DATABASE':
                raise ValueError('Invalid CREATE DATABASE statement')

            database_name = parsed.args['this'].this.this
            database_path = os.path.join(self.base_dir, database_name)

            if os.path.exists(database_path):
                raise ValueError(f'Database {database_name} already exists')

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                await loop.run_in_executor(pool, lambda: os.makedirs(database_path, mode=0o755))
                await loop.run_in_executor(pool, lambda: os.chmod(self.base_dir, 0o755))
            
            return database_name
        
        except Exception as error:
            print(f'Error creating database: {error}')
            raise

    async def drop_database(self, sql_statement: str, current_database: str) -> None:
        """Drop a database"""
        try:
            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Drop) or parsed.args.get('kind') != 'DATABASE':
                raise ValueError('Invalid DROP DATABASE statement')
            
            database_name = parsed.args['this'].this.this
            database_path = os.path.join(self.base_dir, database_name)

            if not os.path.exists(database_path):
                raise ValueError(f'Database {database_name} does not exist')
            
            # if current_database == database_name:
            #     raise ValueError(f'Cannot drop database {database_name} while connected to it')
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                await loop.run_in_executor(pool, lambda: shutil.rmtree(database_path))
        
        except Exception as error:
            print(f'Error dropping database: {error}')
            raise 