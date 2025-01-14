import fs from 'fs/promises';
import path from 'path';
import { Parser } from 'node-sql-parser';
import { CreateTableAst, SqlColumnDefinition, SqlAst } from './types';

interface ColumnDefinition {
  name: string;
  type: string;
}

export class CsvDatabase {
  private baseDir: string;

  constructor(dataDirectory: string = 'data') {
    this.baseDir = dataDirectory;
  }

  async init() {
    try {
      await fs.mkdir(this.baseDir, { recursive: true });
    } catch (error) {
      console.error('Error creating data directory:', error);
      throw error;
    }
  }

  async createTable(sqlStatement: string): Promise<void> {
    try {
      const parser = new Parser();
      const parsedSql = parser.parse(sqlStatement) as SqlAst;
      console.log(parsedSql);

      const ast = parsedSql.ast[0] as CreateTableAst;
      console.log(ast.type);
      console.log(ast.table);
      console.log(ast.table.length);

      if (ast.type !== 'create' || !ast.table || ast.table.length === 0) {
        throw new Error('Invalid CREATE TABLE statement');
      }

      const tableName = ast.table[0].table;
      const columns = this.extractColumns(ast);

      // Create CSV header
      const header = columns.map(col => col.name).join(',');
      const filePath = path.join(this.baseDir, `${tableName}.csv`);

      // Check if file already exists
      try {
        await fs.access(filePath);
        throw new Error(`Table ${tableName} already exists`);
      } catch (error: any) {
        if (error.message.includes('already exists')) {
          throw error;
        }
        // File doesn't exist, create it
        await fs.writeFile(filePath, header + '\n');
      }
    } catch (error) {
      if (error instanceof Error) {
        if (error.message.includes('already exists')) {
          throw error;
        }
        console.error('Error creating table:', error.message);
        throw error;
      }
      throw new Error('Unknown error occurred');
    }
  }

  async insert(sqlStatement: string): Promise<void> {
    // TODO: Implement insert functionality
    console.log('Inserting data into table:', sqlStatement);
  }

  private extractColumns(ast: CreateTableAst): ColumnDefinition[] {
    if (!ast.create_definitions) {
      throw new Error('No columns defined in CREATE TABLE statement');
    }

    return ast.create_definitions.map((def: SqlColumnDefinition) => ({
      name: def.column.column,
      type: def.definition.dataType
    }));
  }
} 