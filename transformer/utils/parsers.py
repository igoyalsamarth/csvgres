from sqlglot import expressions as exp
from typing import List, Any
from ..models.column import ColumnDefinition

class SqlParser:
    def parse_where_expression(self, where_expr) -> str:
        """Convert sqlglot WHERE expression to pandas query syntax"""
        if isinstance(where_expr, exp.Where):
            return self.parse_where_expression(where_expr.this)
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

    def parse_values(self, values_str: str) -> List[Any]:
        """Parse VALUES clause from INSERT statement"""
        # Remove parentheses and split by comma
        values_str = values_str.strip('()')
        return [val.strip().strip("'\"") for val in values_str.split(',')]
