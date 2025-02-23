from utils.database import get_db
from os import getenv
import uuid as uuid4

async def create_database_func(project_id: str, database_name: str) -> dict:
    csvgres = get_db()
    database_id = str(uuid4.uuid4())

    project_data = await csvgres.select(f"SELECT database FROM projects WHERE projectid = '{project_id}'", getenv('DATABASE_NAME'))
    if project_data and project_data[0]['database']:
        database_ids = eval(project_data[0]['database'])
        if database_ids:
            database_ids_str = "','".join(database_ids)
            existing_db = await csvgres.select(f"SELECT database_name FROM databases WHERE database_id IN ('{database_ids_str}') AND database_name = '{database_name['name']}'", getenv('DATABASE_NAME'))
            if existing_db:
                return {"status_code": 400, "detail": f"Database with name {database_name['name']} already exists in this project"}

    await csvgres.create_database(f"CREATE DATABASE {database_name['name']}")
    await csvgres.insert(f"INSERT INTO databases (database_id, database_name) VALUES ('{database_id}', '{database_name['name']}')", getenv('DATABASE_NAME'))
    await csvgres.update_row(f"UPDATE projects SET database = database || '{database_id}' WHERE projectid = '{project_id}'", getenv('DATABASE_NAME'))

async def delete_database_func(project_id: str, database_id: str) -> dict:
    csvgres = get_db()
    database = await csvgres.select(f"SELECT database_name FROM databases WHERE database_id = '{database_id}'", getenv('DATABASE_NAME'))
    if not database:
        raise ValueError(f"Database with id {database_id} not found")
    await csvgres.drop_database(f"DROP DATABASE {database[0]['database_name']}")
    await csvgres.delete_row(f"DELETE FROM databases WHERE database_id = '{database_id}'", getenv('DATABASE_NAME'))
    await csvgres.update_row(f"UPDATE projects SET database = database - '{database_id}' WHERE projectid = '{project_id}'", getenv('DATABASE_NAME'))
    return {"message": "Database deleted successfully"}