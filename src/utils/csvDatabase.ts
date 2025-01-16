import fs from 'fs/promises';
import path from 'path';
import { Parser } from 'node-sql-parser';
import { CreateTableAst, SqlColumnDefinition, SqlAst, InsertAst } from './types';

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
      } catch (error) {
        if (error instanceof Error && error.message.includes('already exists')) {
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
    try {
      const parser = new Parser();
      const parsedSql = parser.parse(sqlStatement) as SqlAst;
      const ast = parsedSql.ast[0] as any;

      if (ast.type !== 'insert') {
        throw new Error('Invalid INSERT statement');
      }

      const tableName = ast.table[0].table;
      const filePath = path.join(this.baseDir, `${tableName}.csv`);

      try {
        await fs.access(filePath);
      } catch (error) {
        throw new Error(`Table ${tableName} does not exist`);
      }

      if (!ast.values || !ast.values[0] || !ast.values[0].value) {
        throw new Error('No values provided in INSERT statement');
      }

      const values = ast.values[0].value.map(val =>
        val.type === 'string' ? `"${val.value}"` : val.value
      ).join(',');

      await fs.appendFile(filePath, values + '\n');
    } catch (error) {
      if (error instanceof Error) {
        console.error('Error inserting data:');
      }
      throw new Error('Unknown error occurred');
    }
  }

  async select(sqlStatement: string): Promise<any[]> {
    try {
      const parser = new Parser();
      const parsedSql = parser.parse(sqlStatement) as SqlAst;
      const ast = parsedSql.ast[0] as any;

      if (ast.type !== 'select') {
        throw new Error('Invalid SELECT statement');
      }

      const tableName = ast.from[0].table;
      const filePath = path.join(this.baseDir, `${tableName}.csv`);

      // Check if file exists
      try {
        await fs.access(filePath);
      } catch (error) {
        throw new Error(`Table ${tableName} does not exist`);
      }

      // Read CSV file
      const fileContent = await fs.readFile(filePath, 'utf-8');
      const lines = fileContent.trim().split('\n');
      const headers = lines[0].split(',');

      // Parse CSV data into array of objects
      const data = lines.slice(1).map(line => {
        const values = this.parseCSVLine(line);
        return headers.reduce((obj, header, index) => {
          obj[header] = values[index];
          return obj;
        }, {} as Record<string, string>);
      });

      // Apply WHERE clause if it exists
      let results = data;
      if (ast.where) {
        results = data.filter(row => this.evaluateWhereClause(row, ast.where));
      }

      // Select only requested columns
      const selectedColumns = ast.columns[0].expr.column === '*'
        ? headers
        : ast.columns.map((col: any) => col.expr.column);

      return results.map(row => {
        if (selectedColumns === headers) return row;
        return selectedColumns.reduce((obj: any, col: string) => {
          obj[col] = row[col];
          return obj;
        }, {});
      });

    } catch (error) {
      if (error instanceof Error) {
        console.error('Error selecting data:', error.message);
        throw error;
      }
      throw new Error('Unknown error occurred');
    }
  }

  private parseCSVLine(line: string): string[] {
    const values: string[] = [];
    let currentValue = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      if (char === '"') {
        inQuotes = !inQuotes;
      } else if (char === ',' && !inQuotes) {
        values.push(currentValue);
        currentValue = '';
      } else {
        currentValue += char;
      }
    }
    values.push(currentValue);
    return values;
  }

  private evaluateWhereClause(row: Record<string, string>, where: any): boolean {
    const evaluateCondition = (condition: any): boolean => {
      const left = condition.left.column ? row[condition.left.column] : condition.left.value;
      const right = condition.right.column ? row[condition.right.column] : condition.right.value;

      // Convert values based on the right operand's type
      const leftValue = condition.right.type === 'number' ? Number(left) : String(left).trim();
      const rightValue = condition.right.type === 'number' ? Number(right) : String(right).trim();

      switch (condition.operator) {
        case '=':
          return leftValue === rightValue;
        case '>':
          return leftValue > rightValue;
        case '<':
          return leftValue < rightValue;
        case '>=':
          return leftValue >= rightValue;
        case '<=':
          return leftValue <= rightValue;
        case '!=':
          return leftValue !== rightValue;
        default:
          throw new Error(`Unsupported operator: ${condition.operator}`);
      }
    };

    return evaluateCondition(where);
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