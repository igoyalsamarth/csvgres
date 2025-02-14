from fastapi import APIRouter, Request
from decorators.auth_decorator import require_auth

# Create router for projects with a prefix
project_router = APIRouter(prefix="/projects")

@project_router.post("")
@require_auth
async def create_project(request: Request):
    user_id = request.auth['user_id']
    project_data = await request.json()
    
    # Process the project creation
    result = await process_project_creation(user_id, project_data)
    
    return result

@project_router.get("") 
@require_auth
async def list_projects(request: Request):

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