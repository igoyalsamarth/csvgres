export interface SqlAst {
  tableList: string[];
  columnList: string[];
  ast: SqlStatement[];
}

export interface SqlStatement {
  type: string;
  keyword: string;
  temporary: null;
  if_not_exists: null;
  table: TableDefinition[];
  ignore_replace: null;
  as: null;
  query_expr: null;
  create_definitions: SqlColumnDefinition[];
  table_options: null;
}

export interface TableDefinition {
  table: string;
  [key: string]: any; // for any additional properties
}

export interface CreateTableAst extends SqlStatement {
  type: 'create';
  create_definitions: SqlColumnDefinition[];
}

export interface SqlColumnDefinition {
  column: {
    column: string;
  };
  definition: {
    dataType: string;
  };
} 