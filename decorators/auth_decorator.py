from functools import wraps
from fastapi import HTTPException, Request
from auth.auth_service import AuthService

_auth_service = AuthService()

def require_auth(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        request = next((arg for arg in args if isinstance(arg, Request)), kwargs.get('request'))
                
        if not request:
            raise HTTPException(
                status_code=500,
                detail="Request object not found in function arguments"
            )
            
        auth_data = _auth_service.verify_auth(request)
        
        if not auth_data:
            raise HTTPException(
                status_code=401,
                detail="Unauthorized"
            )
        
        request.state.auth = auth_data
                
        return await f(*args, **kwargs)
    return decorated_function