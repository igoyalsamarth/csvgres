from fastapi import APIRouter
from typing import List

# Create the main v1 router
v1_router = APIRouter(prefix="/api/v1")

# Import and include the project router
from api.project import project_router
v1_router.include_router(project_router)

# List to keep track of all routers that need to be included
routers: List[APIRouter] = [v1_router]

def get_routers():
    return routers