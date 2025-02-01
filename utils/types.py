from typing import List, Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class TableDefinition:
    table: str
    additional_props: Dict[str, Any] = None  # for any additional properties

@dataclass
class SqlColumnDefinition:
    column: Dict[str, str]  # {"column": column_name}
    definition: Dict[str, str]  # {"dataType": type_name}

@dataclass
class SqlStatement:
    type: str
    keyword: str
    temporary: None = None
    if_not_exists: None = None
    table: List[TableDefinition] = None
    ignore_replace: None = None
    as_statement: None = None  # renamed from 'as' since it's a Python keyword
    query_expr: None = None
    create_definitions: List[SqlColumnDefinition] = None
    table_options: None = None

@dataclass
class CreateTableAst(SqlStatement):
    type: str = 'create'
    create_definitions: List[SqlColumnDefinition] = None

@dataclass
class InsertAst(SqlStatement):
    type: str = 'insert'
    values: List[Any] = None

@dataclass
class SqlAst:
    tableList: List[str]
    columnList: List[str]
    ast: List[SqlStatement] 