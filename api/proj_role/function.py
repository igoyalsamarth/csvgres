import shortuuid
import uuid as uuid4
from os import getenv
from utils.database import get_db

async def create_proj_role_func(proj_id: str, proj_role_name: str) -> dict:
    csvgres = get_db()
    proj_role_id = str(uuid4.uuid4())
    password = shortuuid.ShortUUID().random(length=12)
    await csvgres.insert(f"INSERT INTO proj_role (projroleid, projectid, projectrolename, password) VALUES ('{proj_role_id}', '{proj_id}', '{proj_role_name}', '{password}')", getenv('DATABASE_NAME'))
    return {"status_code": 200, "detail": "Project role created successfully"}

async def update_proj_role_password_func(proj_id: str, proj_role_name: str) -> dict:
    csvgres = get_db()
    password = shortuuid.ShortUUID().random(length=12)
    await csvgres.update_row(f"UPDATE proj_role SET password = '{password}' WHERE projectid = '{proj_id}' AND projectrolename = '{proj_role_name}'", getenv('DATABASE_NAME'))
    return {"status_code": 200, "detail": "Project role password updated successfully"}
