import os
from typing import Optional, TypedDict
from clerk_backend_api import Clerk
from clerk_backend_api.jwks_helpers import authenticate_request, AuthenticateRequestOptions
from dotenv import load_dotenv

class UserAuth(TypedDict):
    user_id: str
    session_id: str
    is_signed_in: bool

class AuthService:
    def __init__(self):
        
        load_dotenv()
        clerk_secret = os.getenv('CLERK_SECRET_KEY')
        
        if not clerk_secret:
            raise ValueError("CLERK_SECRET_KEY environment variable is not set")
            
        self.clerk = Clerk(bearer_auth=clerk_secret)

    def verify_auth(self, request) -> Optional[UserAuth]:
        """
        Verifies the authentication token and returns user information if valid
        """
        try:
            auth_header = request.headers.get('authorization')
            
            if not auth_header or not auth_header.startswith('Bearer '):
                return None
                        
            # Verify the request using Clerk with correct authorized party
            request_state = self.clerk.authenticate_request(
                request,
                AuthenticateRequestOptions(
                    authorized_parties=['http://localhost:3000']  # Use your frontend URL instead of '*'
                )
            )
            
            if not request_state.is_signed_in:
                return None

            # Extract relevant user data from the payload
            auth_data = UserAuth(
                user_id=request_state.payload.get('sub'),
                session_id=request_state.payload.get('sid'),
                is_signed_in=request_state.is_signed_in
            )
            return auth_data

        except Exception as e:
            print(f"Auth error: {str(e)}")
            return None