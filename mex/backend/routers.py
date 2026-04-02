from fastapi import APIRouter, Depends

from mex.backend.security import has_read_access, has_write_access

write_router = APIRouter(dependencies=[Depends(has_write_access)])
read_router = APIRouter(dependencies=[Depends(has_read_access)])
public_router = APIRouter()  # no auth
