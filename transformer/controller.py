import os
import pandas as pd
from .operations.database_ops import DatabaseOperations
from .operations.table_ops import TableOperations
from .operations.data_ops import DataOperations

class Csvgres:
    def __init__(self, data_directory: str = 'data'):
        self.base_dir = data_directory
        self.current_database = None
        
        # Initialize operation classes
        self.db_ops = DatabaseOperations(self.base_dir)
        self.table_ops = TableOperations(self.base_dir)
        self.data_ops = DataOperations(self.base_dir)

    def init(self):
        try:
            os.makedirs(self.base_dir, exist_ok=True)
        except Exception as error:
            print(f'Error creating data directory: {error}')
            raise

    async def create_database(self, sql_statement: str) -> None:
        self.current_database = await self.db_ops.create_database(sql_statement)

    async def drop_database(self, sql_statement: str) -> None:
        await self.db_ops.drop_database(sql_statement, self.current_database)

    async def create_table(self, sql_statement: str, database_name: str = 'csvgres') -> None:
        await self.table_ops.create_table(sql_statement, database_name)

    async def drop_table(self, sql_statement: str, database_name: str = 'csvgres') -> None:
        await self.table_ops.drop_table(sql_statement, database_name)

    async def insert(self, sql_statement: str, database_name: str = 'csvgres') -> None:
        await self.data_ops.insert(sql_statement, database_name)

    async def update_row(self, sql_statement: str, database_name: str = 'csvgres') -> None:
        await self.data_ops.update_row(sql_statement, database_name)

    async def select(self, sql_statement: str, database_name: str = 'csvgres') -> pd.DataFrame:
        return await self.data_ops.select(sql_statement, database_name)

    async def delete_row(self, sql_statement: str, database_name: str = 'csvgres') -> None:
        await self.data_ops.delete_row(sql_statement, database_name)