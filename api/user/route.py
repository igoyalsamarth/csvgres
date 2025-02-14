from fastapi import APIRouter, Request, HTTPException
from decorators.auth_decorator import require_auth
from .function import create_or_update_user
from auth.auth_service import AuthService

# Create router for projects with a prefix
user_router = APIRouter(prefix="/user")

@user_router.post("")
@require_auth
async def create_project(request: Request):
    auth_service = AuthService()
    user = auth_service.verify_auth(request)  # Pass request object instead of user_data
    print(user)
    # user = create_or_update_user(user_data['userId'], user_data['timestamp'])
    # return user
