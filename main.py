from fastapi import FastAPI, HTTPException
from api_utils import *
from database import *
import json
import uuid
from serialize import deserialize

app = FastAPI()
redis_client = Redis().get_client()


@app.post("/register_function", response_model=RegisterFnRep)
def register_function(input: RegisterFn):
    function_id = uuid.uuid4()
    try:
        deserialize(input.payload)
    except:
        raise HTTPException(status_code=400, detail="Function not serialized")
    data = {
        "function_name": input.name,
        "function_payload": input.payload,
    }
    redis_client.hset("functions", str(function_id), json.dumps(data))
    return RegisterFnRep(function_id=function_id)

@app.post("/execute_function", response_model=ExecuteFnRep)
def execute_function(input: ExecuteFnReq):
    function_data = redis_client.hget("functions", str(input.function_id))
    if not function_data:
        raise HTTPException(status_code=404, detail="Function not found")
    
    function_data = json.loads(function_data)
    task_id = uuid.uuid4()
    redis_client.publish("tasks", str(task_id))
    data = {
        "function_payload": function_data["function_payload"],
        "param_payload": input.payload,
        "status": "QUEUED",
        "result": None
    }
    redis_client.hset("tasks", str(task_id), json.dumps(data))
    return ExecuteFnRep(task_id=task_id)
    
@app.get('/status/{task_id}', response_model=TaskStatusRep)
def get_status(task_id: str):
    task_data = redis_client.hget("tasks", task_id)
    if not task_data:
       raise HTTPException(status_code=404, detail="Task not found")
    
    task_data = json.loads(task_data)
    return TaskStatusRep(task_id=task_id, status=task_data["status"])

@app.get('/result/{task_id}', response_model=TaskResultRep)
def get_result(task_id: str):
    task_data = redis_client.hget("tasks", task_id)
    print(task_data)
    if not task_data:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task_data = json.loads(task_data)
    result = task_data["result"]

    '''
    if result is None:
        raise HTTPException(status_code=200, detail="Result not available yet", items = {"task_id" : "crap"})
    '''
    return TaskResultRep(task_id=task_id, status=task_data["status"], result=str(result))
