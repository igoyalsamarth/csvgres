from fastapi import APIRouter, Request, HTTPException
from decorators.auth_decorator import require_auth
from .function import create_or_update_user
from auth.email_service import EmailService

# Create router for projects with a prefix
user_router = APIRouter(prefix="/user")

@user_router.post("")
@require_auth
async def create_project(request: Request):
    user_id = request.state.auth['user_id']
    email_service = EmailService()
    user_email  = email_service.verify_auth(request)
    user = await create_or_update_user(user_id, user_email.get('email'))
    return user