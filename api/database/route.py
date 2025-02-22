from fastapi import APIRouter, Request
from decorators.auth_decorator import require_auth
from .function import create_database_func, delete_database_func

database_router = APIRouter(prefix="/database")

@database_router.post("/{project_id}")
@require_auth
async def create_database(request: Request, project_id: str):
    project_name = await request.json()
    return await create_database_func(project_id, project_name)

@database_router.delete("/delete/{project_id}/{database_id}")
@require_auth
async def delete_database(request: Request, project_id: str, database_id: str):
    return await delete_database_func( project_id, database_id)

