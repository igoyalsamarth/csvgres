import time
from utils.random_word.random_word_generator import get_random_word
from transformer.controller import Csvgres
from os import getenv

async def create_project_func(user_id: str, project_data: dict) -> None:
    csvgres = Csvgres()
    project_id = get_random_word() + '-' + get_random_word() + '-' + str(int(time.time()))[-8:]
    project_name = project_data.get('name')
    
    if not project_name or project_name.strip() == '':
        project_name = project_id
    
    await csvgres.insert(f"INSERT INTO projects (projectid, projectname) VALUES ('{project_id}', '{project_name}')", getenv('DATABASE_NAME'))
    await csvgres.update_row(f"UPDATE users SET projects = projects || '{project_id}' WHERE userid = '{user_id}'", getenv('DATABASE_NAME'))

async def delete_project_func(user_id: str, project_id: str) -> None:
    csvgres = Csvgres()
    await csvgres.update_row(f"UPDATE projects SET deletedat = '{time.strftime('%Y-%m-%dT%H:%M:%S')}' WHERE projectid = '{project_id}'", getenv('DATABASE_NAME'))
    await csvgres.update_row(f"UPDATE users SET projects = projects - '{project_id}' WHERE userid = '{user_id}'", getenv('DATABASE_NAME'))