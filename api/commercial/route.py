from fastapi import APIRouter, Request, HTTPException
from decorators.auth_decorator import require_auth
from .function import run_query
import sqlglot


commercial_router = APIRouter(prefix="/commercial")

@commercial_router.get("")
async def commercial_route():
    try:
        # # Try to get query from raw body first
        # query = await request.body()
        # query = query.decode('utf-8')
        
        # # If empty, try JSON body as fallback
        # if not query:
        #     body = await request.json()
        #     query = body.get("query")
            
        # if not query:
        #     raise HTTPException(status_code=400, detail="Query is required")

        query = """
        SELECT e.first_name, e.last_name, e.salary, d.department_name
        FROM employees e
        JOIN departments d ON e.department_id = d.department_id;
        """
        
        result = await run_query(query)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid SQL query: {str(e)}")
    # except UnicodeDecodeError:
    #     raise HTTPException(status_code=400, detail="Invalid query encoding")
    # except sqlglot.ParseError as e:
    #     raise HTTPException(status_code=400, detail=f"Invalid SQL query: {str(e)}")
    # except asyncpg.InvalidPasswordError:
    #     raise HTTPException(status_code=401, detail="Invalid database credentials")
    # except asyncpg.PostgresError as e:
    #     raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@commercial_router.post("")
async def commercial_route(request: Request):
    try:
        body = await request.json()
        query = body.get("query")
        result = await run_query(query)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid SQL query: {str(e)}")