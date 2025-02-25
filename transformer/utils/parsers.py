from sqlglot import expressions as exp
from typing import List, Any
from ..models.column import ColumnDefinition

class SqlParser:
    def parse_where_expression(self, where_expr) -> str:
        """Convert sqlglot WHERE expression to pandas query syntax"""
        if isinstance(where_expr, exp.Where):
            return self.parse_where_expression(where_expr.this)
        elif isinstance(where_expr, exp.And):
            # Recursively parse both sides of the AND
            left = self.parse_where_expression(where_expr.this)
            right = self.parse_where_expression(where_expr.expression)
            return f"({left}) & ({right})"
        elif isinstance(where_expr, exp.In):
            column = where_expr.this.this
            values = [f"'{expr.this}'" if expr.is_string else expr.this 
                     for expr in where_expr.expressions]
            return f"{column} in [{', '.join(values)}]"
        elif isinstance(where_expr, exp.Is):
            column = where_expr.this.this
            if isinstance(where_expr.expression, exp.Null):
                return f"{column}.isna()"
            return f"not {column}.isna()"
        elif isinstance(where_expr, exp.EQ):
            expr_value = where_expr.expression.this
            column = where_expr.this.this
            if isinstance(where_expr.expression, exp.Literal) and where_expr.expression.is_string:
                # Use `==` with proper string escaping
                escaped_value = expr_value.replace("'", "\\'")
                return f"`{column}` == '{escaped_value}'"
            return f"`{column}` == {expr_value}"
        elif isinstance(where_expr, exp.NEQ):
            expr_value = where_expr.expression.this
            if isinstance(where_expr.expression, exp.Literal) and where_expr.expression.is_string:
                return f"{where_expr.this.this} != '{expr_value}'"
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

    def extract_columns(self, parsed) -> List[ColumnDefinition]:
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

            # Initialize other properties
            not_null = False
            unique = False
            default = None
            array_subtype = None

            # Handle regular data types
            if isinstance(col.kind, exp.DataType):
                data_type = str(col.kind.this).upper()
                if data_type.startswith('TYPE.'):
                    data_type = data_type[5:]
                
                # Handle ARRAY type specifically
                if data_type == 'ARRAY' and hasattr(col.kind, 'expressions'):
                    array_subtype = str(col.kind.expressions[0].this).upper()
                    if array_subtype.startswith('TYPE.'):
                        array_subtype = array_subtype[5:]
                
                # Add size specification if present
                elif hasattr(col.kind, 'expressions') and col.kind.expressions:
                    size = col.kind.expressions[0].this
                    data_type = f"{data_type}({size})"
            else:
                data_type = str(col.kind.this).upper()
                if data_type.startswith('TYPE.'):
                    data_type = data_type[5:]

            # Process constraints
            if hasattr(col, 'constraints') and col.constraints:
                for constraint in col.constraints:
                    if isinstance(constraint.kind, exp.NotNullColumnConstraint):
                        not_null = True
                    elif isinstance(constraint.kind, exp.UniqueColumnConstraint) and not primary_key:
                        unique = True
                    elif isinstance(constraint.kind, exp.DefaultColumnConstraint):
                        if isinstance(constraint.kind.this, exp.CurrentTimestamp):
                            default = "CURRENT_TIMESTAMP"
                        elif isinstance(constraint.kind.this, exp.Null):
                            default = "NULL"
                            not_null = False
                        elif isinstance(constraint.kind.this, exp.Cast):
                            # Handle array defaults
                            if str(constraint.kind.this.to.this).upper() == 'ARRAY':
                                default = []
                        elif isinstance(constraint.kind.this, exp.Array):
                            default = []
                        else:
                            default = constraint.kind.this.this if hasattr(constraint.kind.this, 'this') else constraint.kind.this

            # For primary key columns that aren't SERIAL
            if primary_key:
                not_null = True
                unique = False

            # Build column arguments
            column_args = {
                'name': name,
                'type': data_type,
                'is_serial': False
            }

            # Add array subtype if it's an array
            if data_type == 'ARRAY' and array_subtype:
                column_args['array_subtype'] = array_subtype
                if default is None:
                    default = []
                column_args['default'] = default

            # Only add non-default constraints
            if primary_key:
                column_args['primary_key'] = True
            if not_null and not primary_key:
                column_args['not_null'] = True
            if unique:
                column_args['unique'] = True
            if default is not None and not column_args.get('is_serial'):
                column_args['default'] = default

            columns.append(ColumnDefinition(**column_args))

        return columns

    def parse_values(self, values_str: str) -> List[Any]:
        """Parse VALUES clause from INSERT statement"""
        # Remove parentheses and split by comma
        values_str = values_str.strip('()')
        return [val.strip().strip("'\"") for val in values_str.split(',')]
