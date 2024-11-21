from pydantic import BaseModel
import uuid

# Register Function
class RegisterFn(BaseModel):
    name: str
    payload: str

class RegisterFnRep(BaseModel):
    function_id: uuid.UUID

# Execute Function
class ExecuteFnReq(BaseModel):
    function_id: uuid.UUID
    payload: str

class ExecuteFnRep(BaseModel):
    task_id: uuid.UUID

# Task Status
class TaskStatusRep(BaseModel):
    task_id: uuid.UUID
    status: str

# Task Result
class TaskResultRep(BaseModel):
    task_id: uuid.UUID
    status: str
    result: str