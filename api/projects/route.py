from fastapi import APIRouter, Request
from decorators.auth_decorator import require_auth
from .function import list_projects_func

# Create router for projects with a prefix
projects_router = APIRouter(prefix="/projects")

@projects_router.get("")
@require_auth
async def list_projects(request: Request):
    user_id = request.state.auth['user_id']
    return await list_projects_func(user_id)
