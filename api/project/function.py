import uuid
from datetime import datetime
from utils.random_word.random_word_generator import get_random_word
from transformer.controller import Csvgres
from os import getenv

async def create_project_func(user_id: str, project_data: dict) -> None:
    csvgres = Csvgres()
    project_id = str(uuid.uuid4())
    project_name = project_data.get('projectName', get_random_word() + " " + get_random_word())
    await csvgres.insert(f"INSERT INTO projects (projectId, projectName, region, database) VALUES ('{project_id}', '{project_name}', 'Mumbai', '[]')", getenv('DATABASE_NAME'))
    await csvgres.update_row(f"UPDATE users SET projects = projects || '{project_id}' WHERE userId = '{user_id}'", getenv('DATABASE_NAME'))
  