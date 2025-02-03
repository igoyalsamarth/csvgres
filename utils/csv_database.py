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
    is_serial: bool = False
    not_null: bool = False
    primary_key: bool = False
    unique: bool = False
    default: Optional[Any] = None
    initial_counter_value: Optional[int] = 1
    auto_increment_counter: Optional[int] = 1  # For SERIAL columns

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
            if self.current_database is None:
                raise ValueError('Not connected to any database. Use connect command first.')

            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Create) or parsed.args.get('kind') != 'TABLE':
                raise ValueError('Invalid CREATE TABLE statement')

            table_name = parsed.args['this'].this.this
            database_path = os.path.join(self.base_dir, self.current_database)
            metadata_path = os.path.join(database_path, '.metadata')  # Hidden metadata directory
            data_path = os.path.join(database_path, 'tables')        # Data directory

            # Create metadata and data directories if they don't exist
            os.makedirs(metadata_path, exist_ok=True)
            os.makedirs(data_path, exist_ok=True)

            file_path = os.path.join(data_path, f'{table_name}.csv')
            meta_path = os.path.join(metadata_path, f'{table_name}.json')
            
            if os.path.exists(file_path):
                raise ValueError(f'Table {table_name} already exists')
            
            columns = self._extract_columns(parsed)

            # Create metadata dictionary with only necessary column information
            metadata = {
                'columns': {}
            }
            
            for col in columns:
                col_meta = {
                    'type': col.type
                }
                
                # Only add is_serial if True
                if col.is_serial:
                    col_meta['is_serial'] = True
                    col_meta['initial_counter_value'] = col.initial_counter_value or 1
                    col_meta['auto_increment_counter'] = col.initial_counter_value or 1  # Use initial value if provided
                
                # Only add non-redundant fields
                if col.primary_key:
                    col_meta['primary_key'] = True
                elif col.not_null:  # Only add if not primary key (since PK implies NOT NULL)
                    col_meta['not_null'] = True
                
                if not col.primary_key and col.unique:  # Only add if not primary key (since PK implies UNIQUE)
                    col_meta['unique'] = True
                    
                if col.default is not None and not col.is_serial:  # Don't include default for SERIAL
                    col_meta['default'] = col.default
                    
                metadata['columns'][col.name] = col_meta
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                # Create empty DataFrame with specified columns
                df = pd.DataFrame(columns=[col.name for col in columns])
                # Save CSV and metadata concurrently
                await asyncio.gather(
                    loop.run_in_executor(pool, lambda: df.to_csv(file_path, index=False)),
                    loop.run_in_executor(pool, lambda: self._save_metadata(meta_path, metadata))
                )
        
        except Exception as error:
            print(f'Error creating table: {error}')
            raise

    async def insert(self, sql_statement: str) -> None:
        """Insert data into table from INSERT statement"""
        try:
            if self.current_database is None:
                raise ValueError('Not connected to any database. Use connect command first.')

            parsed = sqlglot.parse_one(sql_statement)
            if not isinstance(parsed, exp.Insert):
                raise ValueError('Invalid INSERT statement')

            table_name = parsed.args['this'].this.this
            database_path = os.path.join(self.base_dir, self.current_database)
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
                # Validate that all specified columns exist
                for col in provided_columns:
                    if col not in columns_meta:
                        raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")
            else:
                # If no columns specified, use all non-SERIAL columns
                provided_columns = [col_name for col_name, col_meta in columns_meta.items() 
                                  if not col_meta.get('is_serial', False)]

            values_list = []
            if isinstance(parsed.args['expression'], exp.Values):
                for tuple_expr in parsed.args['expression'].expressions:
                    row_values = {}
                    
                    # First, initialize all columns with defaults or NULL
                    for col_name, col_meta in columns_meta.items():
                        if col_meta.get('is_serial', False):
                            # Handle SERIAL columns
                            row_values[col_name] = col_meta['auto_increment_counter']
                            col_meta['auto_increment_counter'] += 1
                        elif 'default' in col_meta:
                            row_values[col_name] = self._parse_value_with_type(
                                col_meta['default'], 
                                col_meta['type']
                            )
                        else:
                            row_values[col_name] = None

                    # Then fill in provided values for non-SERIAL columns
                    for i, val in enumerate(tuple_expr.expressions):
                        col_name = provided_columns[i]
                        col_meta = columns_meta[col_name]
                        
                        # Skip SERIAL columns as they are auto-generated
                        if not col_meta.get('is_serial', False):
                            # Get the raw value
                            raw_value = val.this if isinstance(val, exp.Literal) else val.this.this
                            
                            # For string literals, we need to handle them differently
                            if isinstance(val, exp.Literal):
                                if val.is_string and 'INT' in col_meta['type'].upper():
                                    raise ValueError(f"Invalid integer value: String literal '{raw_value}' cannot be used for INT column '{col_name}'")
                            
                            parsed_value = self._parse_value_with_type(raw_value, col_meta['type'])
                            row_values[col_name] = parsed_value

                    # Validate constraints
                    for col_name, value in row_values.items():
                        col_meta = columns_meta[col_name]
                        if value is None and (col_meta.get('not_null', False) or col_meta.get('primary_key', False)):
                            raise ValueError(f"Column '{col_name}' cannot be NULL")

                    values_list.append(row_values)

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                # Read existing data to validate unique constraints
                df = await loop.run_in_executor(pool, pd.read_csv, file_path)
                new_rows = pd.DataFrame(values_list)
                combined_df = pd.concat([df, new_rows], ignore_index=True)
                
                # Validate unique and primary key constraints
                for col_name, col_meta in columns_meta.items():
                    if col_meta.get('unique', False) or col_meta.get('primary_key', False):
                        duplicates = combined_df[col_name].duplicated()
                        if duplicates.any():
                            raise ValueError(f"Duplicate value in {'primary key' if col_meta.get('primary_key', False) else 'unique'} column '{col_name}'")
                
                # If all validations pass, save the data and metadata
                await asyncio.gather(
                    loop.run_in_executor(pool, lambda: combined_df.to_csv(file_path, index=False)),
                    loop.run_in_executor(pool, lambda: self._save_metadata(meta_path, metadata))
                )
        
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
            file_path = os.path.join(self.base_dir, self.current_database, 'tables', f'{table_name}.csv')

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
            file_path = os.path.join(self.base_dir, self.current_database, 'tables', f'{table_name}.csv')

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
            data_path = os.path.join(self.base_dir, self.current_database, 'tables')
            metadata_path = os.path.join(self.base_dir, self.current_database, '.metadata')
            
            file_path = os.path.join(data_path, f'{table_name}.csv')
            meta_path = os.path.join(metadata_path, f'{table_name}.json')

            if not os.path.exists(file_path):
                raise ValueError(f'Table {table_name} does not exist')

            # Run file deletions in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                await asyncio.gather(
                    loop.run_in_executor(pool, os.remove, file_path),
                    loop.run_in_executor(pool, os.remove, meta_path) if os.path.exists(meta_path) else None
                )
            
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
        for col in parsed.args['this'].expressions:
            name = col.this.this
            
            # Check if it's SERIAL by looking at col.kind directly
            is_serial = str(col.kind).lower() == 'serial'
            primary_key = False
            initial_value = None
            
            # First check for primary key constraint since it affects other properties
            if hasattr(col, 'constraints') and col.constraints:
                for constraint in col.constraints:
                    if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                        primary_key = True
                        break
                    # Check for START WITH clause in constraints
                    elif isinstance(constraint.kind, exp.DefaultColumnConstraint):
                        if hasattr(constraint.kind.this, 'this'):
                            try:
                                initial_value = int(constraint.kind.this.this)
                            except (ValueError, TypeError):
                                pass

            # Set the data type and base properties
            if is_serial:
                data_type = 'INT'  # SERIAL columns are actually INTEGER type
                # For SERIAL columns, include initial counter value if specified
                columns.append(ColumnDefinition(
                    name=name,
                    type=data_type,
                    is_serial=True,
                    primary_key=primary_key,
                    initial_counter_value=initial_value,
                    auto_increment_counter=initial_value if initial_value is not None else 1
                ))
                continue

            # Handle regular data types
            if isinstance(col.kind.this, exp.DataType):
                raw_type = str(col.kind.this).upper()
                if raw_type.startswith('TYPE.'):
                    raw_type = raw_type[5:]
                data_type = raw_type
                # Add size specification if present
                if hasattr(col.kind, 'expressions') and col.kind.expressions:
                    size = col.kind.expressions[0].this
                    data_type = f"{data_type}({size})"
            else:
                data_type = str(col.kind.this).upper()
                if data_type.startswith('TYPE.'):
                    data_type = data_type[5:]
            
            # Initialize other properties
            not_null = False
            unique = False
            default = None

            # Process remaining constraints
            if hasattr(col, 'constraints') and col.constraints:
                for constraint in col.constraints:
                    if isinstance(constraint.kind, exp.NotNullColumnConstraint):
                        not_null = True
                    elif isinstance(constraint.kind, exp.UniqueColumnConstraint) and not primary_key:
                        unique = True
                    elif isinstance(constraint.kind, exp.DefaultColumnConstraint):
                        if isinstance(constraint.kind.this, exp.CurrentTimestamp):
                            default = "CURRENT_TIMESTAMP"
                        else:
                            default = constraint.kind.this.this if hasattr(constraint.kind.this, 'this') else constraint.kind.this

            # For primary key columns that aren't SERIAL
            if primary_key:
                not_null = True  # Implied by primary key
                unique = False   # Implied by primary key

            # Create the column definition with only necessary fields
            column_args = {
                'name': name,
                'type': data_type,
                'is_serial': False
            }

            # Only add non-default constraints
            if primary_key:
                column_args['primary_key'] = True
            if not_null and not primary_key:  # Don't include if primary key already implies it
                column_args['not_null'] = True
            if unique:  # Will only be True if not primary key
                column_args['unique'] = True
            if default is not None:
                column_args['default'] = default

            columns.append(ColumnDefinition(**column_args))

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

    def _parse_value_with_type(self, value: Any, data_type: str) -> Any:
        """Convert value to the specified SQL data type"""
        try:
            if value is None:
                return None
                
            # If value is a string with quotes, remove them
            if isinstance(value, str):
                value = value.strip("'\"")
                
            # Normalize the data type string
            data_type = data_type.upper()
            
            # Handle VARCHAR and other string types
            if 'CHAR' in data_type or 'TEXT' in data_type:
                return str(value)
                
            # Handle INTEGER types - strict parsing for strings
            if 'INT' in data_type:
                if isinstance(value, str) and not value.isdigit():
                    raise ValueError(f"Invalid integer value: '{value}'")
                return int(value)
                
            # Handle DECIMAL/NUMERIC types - strict parsing for strings
            if 'DECIMAL' in data_type or 'NUMERIC' in data_type:
                try:
                    return float(value)
                except ValueError:
                    raise ValueError(f"Invalid decimal value: '{value}'")
                
            # Handle BOOLEAN type
            if data_type == 'BOOLEAN':
                if isinstance(value, str):
                    if value.lower() in ('true', 't', 'yes', 'y', '1'):
                        return True
                    elif value.lower() in ('false', 'f', 'no', 'n', '0'):
                        return False
                    raise ValueError(f"Invalid boolean value: '{value}'")
                return bool(value)
                
            # Handle DATE/TIMESTAMP types
            if 'TIMESTAMP' in data_type or 'DATE' in data_type:
                if value == 'CURRENT_TIMESTAMP':
                    from datetime import datetime
                    return datetime.now().isoformat()
                return value
                
            # Default case: return as-is
            return value
            
        except (ValueError, TypeError) as e:
            raise ValueError(f"Cannot convert value '{value}' to type {data_type}: {str(e)}")
