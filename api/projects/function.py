from utils.database import get_db
from os import getenv

async def list_projects_func(user_id: str) -> list:
    csvgres = get_db()
    
    user_data = await csvgres.select(f"SELECT projects FROM users WHERE userid = '{user_id}'", getenv('DATABASE_NAME'))
    if not user_data:
        return []
    
    project_ids = user_data[0]['projects']
    if not project_ids:
        return []

    project_ids_str = "','".join(eval(project_ids))
    projects = await csvgres.select(f"SELECT * FROM projects WHERE projectid IN ('{project_ids_str}') AND deletedat IS NULL", getenv('DATABASE_NAME'))
    if projects:
        for project in projects:
            del project['deletedat']
            del project['database']
        return projects
    return []