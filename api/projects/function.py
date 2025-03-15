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
    projects = await csvgres.select(f"SELECT projectid, projectname, region, database, createdat FROM projects WHERE projectid IN ('{project_ids_str}') AND deletedat IS NULL", getenv('DATABASE_NAME'))
    if projects:
        project_ids_for_db = []
        for project in projects:
            db_ids = eval(project['database'])
            project_ids_for_db.extend(db_ids)
        
        if project_ids_for_db:
            db_ids_str = "','".join(project_ids_for_db)
            all_databases = await csvgres.select(f"SELECT database_id, storage, compute, data_transfer FROM databases WHERE database_id IN ('{db_ids_str}') AND deleted_at IS NULL", getenv('DATABASE_NAME'))
        else:
            all_databases = []

        for project in projects:
            project_db_ids = eval(project['database'])
            project['databases'] = []
            del project['database']
            for db_id in project_db_ids:
                matching_db = next((db for db in all_databases if db['database_id'] == db_id), None)
                if matching_db:
                    project['databases'].append(matching_db)

        return projects
    return []