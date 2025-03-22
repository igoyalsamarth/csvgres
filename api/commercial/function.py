import sqlglot
from sqlglot import expressions as exp
from transformer.controller import Csvgres
import asyncio
from utils.database import get_db
async def run_query(query: str) -> dict:
    """
    Automatically routes and executes SQL queries based on their type.
    Returns a dictionary with the operation result.
    """
    try:

        # Parse the SQL statement
        parsed = sqlglot.parse_one(query)
        csvgres = get_db()

        response = {
            'success': True,
            'result': None,
            'message': None
        }
        print(parsed.args)

        # Route to appropriate operation based on query type
        if isinstance(parsed, exp.Create):
            if parsed.args.get('kind') == 'DATABASE':
                database_name = await csvgres.create_database(query)
                response['message'] = f'Created Database {database_name}'
            elif parsed.args.get('kind') == 'TABLE':
                await csvgres.create_table(query)
                response['message'] = f'Created Table {parsed.args["this"].this.this}'

        elif isinstance(parsed, exp.Drop):
            if parsed.args.get('kind') == 'DATABASE':
                await csvgres.drop_database(query)
                response['message'] = f'Dropped Database {parsed.args["this"].this.this}'
            elif parsed.args.get('kind') == 'TABLE':
                await csvgres.drop_table(query)
                response['message'] = f'Dropped Table {parsed.args["this"].this.this}'

        elif isinstance(parsed, exp.Insert):
            await csvgres.insert(query)
            response['message'] = f'Inserted into Table {parsed.args["this"].this.this}'

        elif isinstance(parsed, exp.Select):
            result = await csvgres.select(query)
            response['result'] = {'rows': result}
            response['message'] = f'Selected from Table {parsed.args["from"].this.this.this}'

        elif isinstance(parsed, exp.Update):
            await csvgres.update_row(query)
            response['message'] = f'Updated Table {parsed.args["this"].this.this}'

        elif isinstance(parsed, exp.Delete):
            await csvgres.delete_row(query)
            response['message'] = f'Deleted from Table {parsed.args["this"].this.this}'

        else:
            raise ValueError(f'Unsupported SQL operation: {type(parsed)}')

        return response

    except Exception as error:
        return {
            'success': False,
            'result': None,
            'message': str(error)
        }