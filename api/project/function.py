import time
from utils.database import get_db
from utils.random_word.random_word_generator import get_random_word
from os import getenv

async def get_project_func(project_id: str) -> dict:
    csvgres = get_db()
    projects = await csvgres.select(f"SELECT projectid, projectname, region, database, createdat FROM projects WHERE projectid = '{project_id}' AND deletedat IS NULL", getenv('DATABASE_NAME'))
    if projects:
        project = projects[0]
        db_ids = eval(project['database'])
        project_ids_for_db = db_ids
        
        if project_ids_for_db:
            db_ids_str = "','".join(project_ids_for_db)
            all_databases = await csvgres.select(f"SELECT database_id, storage, compute, data_transfer FROM databases WHERE database_id IN ('{db_ids_str}') AND deleted_at IS NULL", getenv('DATABASE_NAME'))
        else:
            all_databases = []

        project['databases'] = []
        del project['database']
        for db_id in db_ids:
            matching_db = next((db for db in all_databases if db['database_id'] == db_id), None)
            if matching_db:
                project['databases'].append(matching_db)

        return project
    return {}

async def create_project_func(user_id: str, project_data: dict) -> None:
    csvgres = get_db()
    project_id = get_random_word() + '-' + get_random_word() + '-' + str(int(time.time()))[-8:]
    project_name = project_data.get('name')
    
    if not project_name or project_name.strip() == '':
        project_name = project_id
    
    await csvgres.insert(f"INSERT INTO projects (projectid, projectname) VALUES ('{project_id}', '{project_name}')", getenv('DATABASE_NAME'))
    await csvgres.update_row(f"UPDATE users SET projects = projects || '{project_id}' WHERE userid = '{user_id}'", getenv('DATABASE_NAME'))

async def delete_project_func(user_id: str, project_id: str) -> None:
    csvgres = get_db()
    await csvgres.update_row(f"UPDATE projects SET deletedat = '{time.strftime('%Y-%m-%dT%H:%M:%S')}' WHERE projectid = '{project_id}'", getenv('DATABASE_NAME'))
    await csvgres.update_row(f"UPDATE users SET projects = projects - '{project_id}' WHERE userid = '{user_id}'", getenv('DATABASE_NAME'))