from typing import Any

class TypeHandler:
    def parse_value_with_type(self, value: Any, data_type: str) -> Any:
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
