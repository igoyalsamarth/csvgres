from fastapi import APIRouter, Request
from decorators.auth_decorator import require_auth
from .function import list_databases_func

databases_router = APIRouter(prefix="/databases")

@databases_router.get("/{project_id}")
@require_auth
async def list_databases(request: Request, project_id: str):
    return await list_databases_func(project_id)
