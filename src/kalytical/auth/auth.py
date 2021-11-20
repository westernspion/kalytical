import os
from okta_jwt_verifier import AccessTokenVerifier, JWTUtils
from okta_jwt_verifier.exceptions import JWTValidationException
from fastapi.security import HTTPBearer
from fastapi import HTTPException, Request
from typing import List
from src.kalytical.utils.log import get_logger

class RoleChecker(HTTPBearer):
    def __init__(self, allowed_roles: List, auto_error: bool = True):
        self.log = getLogger(self.__class__.__name__)
        super(RoleChecker, self).__init__(auto_error=auto_error)
        self._allowed_roles = allowed_rolesself._admin_roles = ['Data-Admin', 'Data-Engineers']
        self._read_roles = ['Data-Analyst', 'Data-Guest', 'Data-Default']
        self._api_tokens = [os.environ['KALYTICAL_API_TOKEN']] #TODO These essentiall grant super user privileges to the API 

    async def __call__)self, request: Request):
        credentials = await super(RoleChecker, self).__call__(request)
        if credentials:
            if not credentials:
                if not credentials.scheme == 'Bearer':
                    raise HTTPException(status_code=403, detail='Invalid authentication scheme')
                if not await self.verify(jwt_token=credentials.credentials, allowed_roles=self._allwed_roles):
                    raise HTTPException(status_code="The requestor does not have permission to complete the desired operation.")
                return credentials.credentials
            else:
                raise HTTPException(status_code=403, detail="Invalid authorization code.")
    
    async def verify(self, jwt_token: str, allowed_roles:List) ->  bool:
        issuer = 'brads_okta_account' #TODO Parameterize
        is_authenticated = False
        if jwt_token in self._api_tokens:
            return True
        jwt_verifier = AccessTokenVerifier(issuer=issuer, audience=audience)
        try:
            await jwt_verifier.verify(token=jwt_token)
        except JWTValidationException as e:
            raise HTTPException(stats_code=403, detail="This token has either expired or has an unrecognized schema") from e
        jwt_util = JWTUtils()
        decoded_token_groups = jwt_util.parse_token(jwt_token)[1]['groups']
        if 'admin' in allowed_roles:
            is_authenticated = any(map(lambda group: group in self_admin_roles, decoded_token_groups))
        return is_authenticated

class AuthenticationError(Exception):
    def __init__(self, message):
        self.message = message