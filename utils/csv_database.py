import os
import pandas as pd
from sqlparse import parse
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import sqlglot
from sqlglot import expressions as exp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import shutil

@dataclass
class ColumnDefinition:
    name: str
    type: str

class CsvDatabase:
    def __init__(self, data_directory: str = 'data'):
        self.base_dir = data_directory
        self.current_database = None  # Add this to track current database

    def init(self):
        """Initialize the database directory"""
        try:
            os.makedirs(self.base_dir, exist_ok=True)
        except Exception as error:
            print(f'Error creating data directory: {error}')
            raise

    async def create_database(self, sql_statement: str) -> None:
        """Create a new database from CREATE DATABASE statement"""
        try:
            parsed = sqlglot.parse_one(sql_statement)

            if not isinstance(parsed, exp.Create) or parsed.args.get('kind') != 'DATABASE':
                raise ValueError('Invalid CREATE DATABASE statement')

            database_name = parsed.args['this'].this.this
            database_path = os.path.join(self.base_dir, database_name)

            if os.path.exists(database_path):
                raise ValueError(f'Database {database_name} already exists')

            # Run directory creation in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                # Create directory with mode 0o755 (rwxr-xr-x)
                await loop.run_in_executor(pool, lambda: os.makedirs(database_path, mode=0o755))
                # Ensure parent directory also has correct permissions
                await loop.run_in_executor(pool, lambda: os.chmod(self.base_dir, 0o755))
        
        except Exception as error:
            print(f'Error creating database: {error}')
            raise

    async def connect_database(self, command: str) -> None:
        """Connect to a database from a c or connect command"""
        try:
            # Strip whitespace and split command
            parts = command.strip().split()
            
            # Check if command starts with any valid prefix
            valid_prefixes = [r'\c', r'\connect', 'c', 'connect']
            if not any(parts[0] == prefix for prefix in valid_prefixes):
                raise ValueError('Invalid connect command. Use "c dbname" or "connect dbname"')

            if len(parts) != 2:
                raise ValueError('Invalid connect command. Use "c dbname" or "connect dbname"')

            database_name = parts[1]
            database_path = os.path.join(self.base_dir, database_name)

            if not os.path.exists(database_path):
                raise ValueError(f'Database {database_name} does not exist')
            
            if not os.path.isdir(database_path):
                raise ValueError(f'{database_name} is not a valid database')

            self.current_database = database_name
            
        except Exception as error:
            print(f'Error connecting to database: {error}')
            raise

    async def create_table(self, sql_statement: str) -> None:
        """Create a new table from CREATE TABLE statement"""
        try:
            # Check if connected to a database
            if self.current_database is None:
                raise ValueError('Not connected to any database. Use connect command first.')

            parsed = sqlglot.parse_one(sql_statement)

            if not isinstance(parsed, exp.Create) or parsed.args.get('kind') != 'TABLE':
                raise ValueError('Invalid CREATE TABLE statement')

            # Extract table name and columns
            table_name = parsed.args['this'].this.this
            columns = self._extract_columns(parsed)

            print(f"Creating table {table_name} in database {self.current_database} with columns {columns}")
            
            # Use the current database path
            database_path = os.path.join(self.base_dir, self.current_database)
            file_path = os.path.join(database_path, f'{table_name}.csv')
            
            # Check if file exists
            if os.path.exists(file_path):
                raise ValueError(f'Table {table_name} already exists')
            
            # Create empty DataFrame with specified columns and save to CSV using thread pool
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                df = pd.DataFrame(columns=[col.name for col in columns])
                await loop.run_in_executor(pool, lambda: df.to_csv(file_path, index=False))
        
        except Exception as error:
            print(f'Error creating table: {error}')
            raise

    async def insert(self, sql_statement: str) -> None:
        """Insert data into table from INSERT statement"""
        try:
            # Check if connected to a database
            if self.current_database is None:
                raise ValueError('Not connected to any database. Use connect command first.')

            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Insert):
                raise ValueError('Invalid INSERT statement')

            # Extract table name
            table_name = parsed.args['this'].this.this
            file_path = os.path.join(self.base_dir, self.current_database, f'{table_name}.csv')
            
            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')

            # Extract values from the INSERT statement
            values_list = []
            if isinstance(parsed.args['expression'], exp.Values):
                for tuple_expr in parsed.args['expression'].expressions:
                    row_values = [
                        val.this if isinstance(val, exp.Literal) else val.this.this
                        for val in tuple_expr.expressions
                    ]
                    values_list.append(row_values)

            # Run pandas operations in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                # Read existing CSV
                df = await loop.run_in_executor(pool, pd.read_csv, file_path)
                
                # Append new values
                new_rows = pd.DataFrame(values_list, columns=df.columns)
                df = pd.concat([df, new_rows], ignore_index=True)
                
                # Save updated CSV
                await loop.run_in_executor(pool, lambda: df.to_csv(file_path, index=False))
            
        except Exception as error:
            print(f'Error inserting data: {error}')
            raise

    async def select(self, sql_statement: str) -> pd.DataFrame:
        """Execute SELECT statement and return results"""
        try:
            # Check if connected to a database
            if self.current_database is None:
                raise ValueError('Not connected to any database. Use connect command first.')

            # Parse the SQL into an AST
            parsed = sqlglot.parse_one(sql_statement)

            if not isinstance(parsed, exp.Select):
                raise ValueError('Invalid SELECT statement')

            # Extract table name from the From expression
            from_expr = parsed.args['from']
            table_name = from_expr[0].this.this
            file_path = os.path.join(self.base_dir, self.current_database, f'{table_name}.csv')

            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')

            # Run pandas operations in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                # Read the CSV file into a DataFrame
                df = await loop.run_in_executor(pool, pd.read_csv, file_path)  

                # Handle WHERE clause
                if parsed.args.get('where'):  # Use .get() to safely check for 'where' key
                    condition = self._parse_where_expression(parsed.args['where'])
                    df = await loop.run_in_executor(pool, lambda: df.query(condition))

                # Handle column selection
                if isinstance(parsed.expressions[0], exp.Star):
                    return df
                else:
                    columns = [expr.this.this if isinstance(expr, exp.Column) 
                              else expr.alias_or_name for expr in parsed.expressions]
                    return df[columns]

        except Exception as error:
            print(f'Error selecting data: {error}')
            raise

    async def delete_row(self, sql_statement: str) -> None:
        """Delete a row from a table"""
        try:
            # Check if connected to a database
            if self.current_database is None:
                raise ValueError('Not connected to any database. Use connect command first.')

            parsed = sqlglot.parse_one(sql_statement)
            
            if not isinstance(parsed, exp.Delete):
                raise ValueError('Invalid DELETE statement')

            table_name = parsed.args['this'].this.this
            file_path = os.path.join(self.base_dir, self.current_database, f'{table_name}.csv')

            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')

            # Run pandas operations in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                df = await loop.run_in_executor(pool, pd.read_csv, file_path)

                # Handle WHERE clause
                if parsed.args.get('where'):
                    condition = self._parse_where_expression(parsed.args['where'])
                    # Keep rows that DON'T match the condition (i.e., delete the matching ones)
                    mask = ~df.eval(condition)  # Using ~ to invert the boolean mask
                    df = df[mask]
                else:
                    # If no WHERE clause, delete all rows by creating empty DataFrame with same columns
                    df = pd.DataFrame(columns=df.columns)
                
                # Save the updated DataFrame
                await loop.run_in_executor(pool, lambda: df.to_csv(file_path, index=False))

        except Exception as error:
            print(f'Error deleting row: {error}')
            raise

    async def drop_table(self, sql_statement: str) -> None:
        """Drop a table from a database"""
        try:
            # Check if connected to a database
            if self.current_database is None:
                raise ValueError('Not connected to any database. Use connect command first.')

            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Drop) or parsed.args.get('kind') != 'TABLE':
                raise ValueError('Invalid DROP TABLE statement')

            table_name = parsed.args['this'].this.this
            file_path = os.path.join(self.base_dir, self.current_database, f'{table_name}.csv')

            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')

            os.remove(file_path)
            
        except Exception as error:
            print(f'Error dropping table: {error}')
            raise

    async def drop_database(self, sql_statement: str) -> None:
        """Drop a database"""
        try:
            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Drop) or parsed.args.get('kind') != 'DATABASE':
                raise ValueError('Invalid DROP DATABASE statement')
            
            database_name = parsed.args['this'].this.this
            database_path = os.path.join(self.base_dir, database_name)

            if not os.path.exists(database_path):
                raise ValueError(f'Database {database_name} does not exist')
            
            # Prevent dropping the currently connected database
            if self.current_database == database_name:
                raise ValueError(f'Cannot drop database {database_name} while connected to it. Please connect to a different database first.')
            
            # Use shutil.rmtree instead of os.rmdir to remove directory and all contents
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                await loop.run_in_executor(pool, lambda: shutil.rmtree(database_path))
        
        except Exception as error:
            print(f'Error dropping database: {error}')
            raise

    def _extract_columns(self, parsed) -> List[ColumnDefinition]:
        """Extract column definitions from CREATE TABLE statement"""
        columns = []
        # Get the column definitions from the schema
        for col in parsed.args['this'].expressions:
            name = col.this.this  # Column name
            data_type = col.kind.this.name  # Data type
            columns.append(ColumnDefinition(name=name, type=data_type))
        return columns

    def _parse_values(self, values_str: str) -> List[Any]:
        """Parse VALUES clause from INSERT statement"""
        # Remove parentheses and split by comma
        values_str = values_str.strip('()')
        return [val.strip().strip("'\"") for val in values_str.split(',')]

    def _parse_where_expression(self, where_expr) -> str:
        """Convert sqlglot WHERE expression to pandas query syntax"""
        if isinstance(where_expr, exp.Where):
            return self._parse_where_expression(where_expr.this)
        elif isinstance(where_expr, exp.EQ):
            # Handle string literals by adding quotes
            expr_value = where_expr.expression.this
            if isinstance(where_expr.expression, exp.Literal) and where_expr.expression.is_string:
                expr_value = f"'{expr_value}'"
            return f"{where_expr.this.this} == {expr_value}"
        elif isinstance(where_expr, exp.NEQ):
            expr_value = where_expr.expression.this
            if isinstance(where_expr.expression, exp.Literal) and where_expr.expression.is_string:
                expr_value = f"'{expr_value}'"
            return f"{where_expr.this.this} != {expr_value}"
        elif isinstance(where_expr, exp.GT):
            return f"{where_expr.this.this} > {where_expr.expression.this}"
        elif isinstance(where_expr, exp.LT):
            return f"{where_expr.this.this} < {where_expr.expression.this}"
        elif isinstance(where_expr, exp.GTE):
            return f"{where_expr.this.this} >= {where_expr.expression.this}"
        elif isinstance(where_expr, exp.LTE):
            return f"{where_expr.this.this} <= {where_expr.expression.this}"
        else:
            raise ValueError(f"Unsupported WHERE condition: {type(where_expr)}")
