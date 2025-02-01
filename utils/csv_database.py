import os
import pandas as pd
from sqlparse import parse
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import sqlglot
from sqlglot import expressions as exp
import asyncio
from concurrent.futures import ThreadPoolExecutor

@dataclass
class ColumnDefinition:
    name: str
    type: str

class CsvDatabase:
    def __init__(self, data_directory: str = 'data'):
        self.base_dir = data_directory

    def init(self):
        """Initialize the database directory"""
        try:
            os.makedirs(self.base_dir, exist_ok=True)
        except Exception as error:
            print(f'Error creating data directory: {error}')
            raise

    def create_table(self, sql_statement: str) -> None:
        """Create a new table from CREATE TABLE statement"""
        try:
            # Parse the SQL statement
            parsed = parse(sql_statement)[0]
            if not parsed.get_type() == 'CREATE':
                raise ValueError('Invalid CREATE TABLE statement')

            # Extract table name and columns
            table_name = parsed.tokens[4].get_name()
            columns = self._extract_columns(parsed)
            
            file_path = os.path.join(self.base_dir, f'{table_name}.csv')
            
            # Check if file exists
            if os.path.exists(file_path):
                raise ValueError(f'Table {table_name} already exists')
            
            # Create empty DataFrame with specified columns
            df = pd.DataFrame(columns=[col.name for col in columns])
            df.to_csv(file_path, index=False)
            
        except Exception as error:
            print(f'Error creating table: {error}')
            raise

    def insert(self, sql_statement: str) -> None:
        """Insert data into table from INSERT statement"""
        try:
            parsed = parse(sql_statement)[0]
            if not parsed.get_type() == 'INSERT':
                raise ValueError('Invalid INSERT statement')

            # Extract table name and values
            table_name = parsed.tokens[2].get_name()
            file_path = os.path.join(self.base_dir, f'{table_name}.csv')
            
            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')

            # Extract values from the INSERT statement
            values_str = str(parsed.tokens[-1])
            values = self._parse_values(values_str)
            
            # Read existing CSV
            df = pd.read_csv(file_path)
            
            # Append new values
            new_row = pd.DataFrame([values], columns=df.columns)
            df = pd.concat([df, new_row], ignore_index=True)
            
            # Save updated CSV
            df.to_csv(file_path, index=False)
            
        except Exception as error:
            print(f'Error inserting data: {error}')
            raise

    async def select(self, sql_statement: str) -> pd.DataFrame:
        """Execute SELECT statement and return results"""
        try:
            # Parse the SQL into an AST
            parsed = sqlglot.parse_one(sql_statement)

            if not isinstance(parsed, exp.Select):
                raise ValueError('Invalid SELECT statement')

            # Extract table name from the From expression
            from_expr = parsed.args['from']  # Access the from clause directly from args
            table_name = from_expr[0].this.this  # Get table name from the From expression
            file_path = os.path.join(self.base_dir, f'{table_name}.csv')

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

    def _extract_columns(self, parsed) -> List[ColumnDefinition]:
        """Extract column definitions from CREATE TABLE statement"""
        columns = []
        parenthesis = next(token for token in parsed.tokens if token.is_group)
        for token in parenthesis.tokens:
            if token.ttype is None and not str(token).isspace():
                name, data_type = str(token).split()[0:2]
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
            return f"{where_expr.this.this} == {where_expr.expression.this}"
        elif isinstance(where_expr, exp.NEQ):
            return f"{where_expr.this.this} != {where_expr.expression.this}"
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