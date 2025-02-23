from fastapi import APIRouter, Request
from decorators.auth_decorator import require_auth
from .function import create_proj_role_func, update_proj_role_password_func

proj_role_router = APIRouter(prefix="/proj_role")

@proj_role_router.post("/{proj_id}")
@require_auth
async def create_proj_role(request: Request, proj_id: str):
    proj_role_name = await request.json()
    return await create_proj_role_func(proj_id, proj_role_name)

@proj_role_router.put("/reset_password/{proj_id}")
@require_auth
async def reset_password(request: Request, proj_id: str):
    proj_role_name = await request.json()
    return await update_proj_role_password_func( proj_id, proj_role_name)

