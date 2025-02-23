from utils.database import get_db
from os import getenv

async def list_databases_func(project_id: str) -> list:
    csvgres = get_db()
    # Get database IDs from projects table
    project_data = await csvgres.select(f"SELECT database FROM projects WHERE projectid = '{project_id}'", getenv('DATABASE_NAME'))
    if not project_data or not project_data[0]['database']:
        return []
    
    database_ids = eval(project_data[0]['database'])
    if not database_ids:
            return []
    
    database_ids_str = "','".join(database_ids)
    databases = await csvgres.select(f"SELECT * FROM databases WHERE database_id IN ('{database_ids_str}') AND deleted_at IS NULL", getenv('DATABASE_NAME'))
    if databases:
        for database in databases:
            del database['deleted_at']
        return databases
    return []
