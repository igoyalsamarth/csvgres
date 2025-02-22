from fastapi import APIRouter
from typing import List

v1_router = APIRouter(prefix="/api/v1")

from api.project.route import project_router
from api.user.route import user_router
from api.projects.route import projects_router
from api.databases.route import databases_router
from api.database.route import database_router

v1_router.include_router(project_router)
v1_router.include_router(user_router)
v1_router.include_router(projects_router)
v1_router.include_router(databases_router)
v1_router.include_router(database_router)

routers: List[APIRouter] = [v1_router]

def get_routers():
    return routers