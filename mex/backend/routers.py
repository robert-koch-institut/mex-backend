from fastapi import APIRouter, Depends

from mex.backend.security import (
    has_read_access,
    has_write_access,
    has_write_access_ldap,
)

write_router = APIRouter(dependencies=[Depends(has_write_access)])
read_router = APIRouter(dependencies=[Depends(has_read_access)])
read_router_talking_to_ldap = APIRouter(dependencies=[Depends(has_read_access)])
ldap_login_router = APIRouter(dependencies=[Depends(has_write_access_ldap)])
public_router = APIRouter()  # no auth
