from transformer.controller import Csvgres
from os import getenv

async def list_projects_func(user_id: str) -> list:
    csvgres = Csvgres()
    # Get user's projects from users table
    user_data = await csvgres.select(f"SELECT projects FROM users WHERE userid = '{user_id}'", getenv('DATABASE_NAME'))
    if not user_data:
        return []
    
    project_ids = user_data[0]['projects']
    if not project_ids:
        return []

    project_ids_str = "','".join(eval(project_ids))
    return await csvgres.select(f"SELECT * FROM projects WHERE projectid IN ('{project_ids_str}')", getenv('DATABASE_NAME'))
