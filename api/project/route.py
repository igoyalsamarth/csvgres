from fastapi import APIRouter, Request
from decorators.auth_decorator import require_auth
from .function import create_project_func

# Create router for projects with a prefix
project_router = APIRouter(prefix="/projects")

@project_router.post("")
@require_auth
async def create_project(request: Request):
    user_id = request.state.auth['user_id']
    project_data = await request.json()
    
    await create_project_func(user_id, project_data)
    
@project_router.get("")
@require_auth
async def list_projects(request: Request):
    user_id = request.state.auth['user_id']
    return [
            {
                "id": "proj_1",
                "name": "Solar Farm Alpha",
                "region": "North America",
                "createdAt": "2023-10-15T09:30:00Z"
            },
            {
                "id": "proj_2", 
                "name": "Wind Park Beta",
                "region": "Europe",
                "createdAt": "2023-10-14T15:45:00Z"
            },
            {
                "id": "proj_3",
                "name": "Hydro Plant Gamma",
                "region": "Asia Pacific",
                "createdAt": "2023-10-13T11:20:00Z"
            },
            {
                "id": "proj_4",
                "name": "Geothermal Site Delta",
                "region": "South America", 
                "createdAt": "2023-10-12T16:15:00Z"
            }
        ]