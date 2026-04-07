from fastapi import APIRouter, Depends

from mex.backend.security import has_write_access

mocked_ldap_router = APIRouter()
database_deletion_router = APIRouter(dependencies=[Depends(has_write_access)])
