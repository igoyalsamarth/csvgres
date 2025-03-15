from fastapi import APIRouter, Request
from decorators.auth_decorator import require_auth
from .function import create_project_func, delete_project_func, get_project_func

# Create router for projects with a prefix
project_router = APIRouter(prefix="/project")

@project_router.get("/{project_id}")
@require_auth
async def get_project(request: Request, project_id: str):
    return await get_project_func(project_id)

@project_router.post("")
@require_auth
async def create_project(request: Request):
    user_id = request.state.auth['user_id']
    project_data = await request.json()
    await create_project_func(user_id, project_data)
    
@project_router.delete("/delete/{project_id}")
@require_auth
async def delete_project(request: Request, project_id: str):
    user_id = request.state.auth['user_id']
    await delete_project_func(user_id, project_id)