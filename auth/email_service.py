import os
from typing import Optional, TypedDict
from clerk_backend_api import Clerk
from clerk_backend_api.jwks_helpers import authenticate_request, AuthenticateRequestOptions
from dotenv import load_dotenv

class UserAuth(TypedDict):
    user_id: str
    email: str

class EmailService:
    def __init__(self):
        
        load_dotenv()
        clerk_secret = os.getenv('CLERK_SECRET_KEY')
        
        if not clerk_secret:
            raise ValueError("CLERK_SECRET_KEY environment variable is not set")
            
        self.clerk = Clerk(bearer_auth=clerk_secret)

    def verify_auth(self, request) -> Optional[UserAuth]:
        """
        Verifies the authentication token and returns user's ID and email if valid
        """
        try:
            auth_header = request.headers.get('authorization')
            
            if not auth_header or not auth_header.startswith('Bearer '):
                print("Invalid or missing Bearer token")
                return None
            
            frontend_url = os.getenv('FRONTEND_URL', 'http://localhost:3000')
            
            # Verify the request using Clerk
            request_state = self.clerk.authenticate_request(
                request,
                AuthenticateRequestOptions(
                    authorized_parties=[frontend_url]
                )
            )
            
            if not request_state.is_signed_in:
                print(f"User is not signed in. Reason: {request_state.reason}")
                return None

            # Get user ID from the request state
            user_id = request_state.payload.get('sub')
            
            # Get user's primary email
            user = self.clerk.users.get(user_id=user_id)
            primary_email_id = user.primary_email_address_id
            email_data = self.clerk.email_addresses.get(email_address_id=primary_email_id)
            
            # Return only user_id and email
            return UserAuth(
                user_id=user_id,
                email=email_data.email_address if email_data else None
            )

        except Exception as e:
            print(f"Auth error: {str(e)}")
            return None
