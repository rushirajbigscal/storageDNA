from fastapi import FastAPI , Request,HTTPException
from fastapi.responses import JSONResponse
import requests
import os
from action_functions import *

app = FastAPI()

@app.post("/send")
async def print_payload(request: Request):
    try:
        payload = await request.json()
        url = payload["downloadableFile"]
        filename = get_filename(url)
        print(filename)
        response = requests.get(url)
        print(response.status_code)
        if response.status_code == 200:
            with open(filename, 'wb') as file:
                file.write(response.content)
            print(f"File download at {filename}")
        else:
            print("Failed")
        
        print("Request Payload:", payload)
        return JSONResponse(
            status_code=200,
            content={"message": "Payload received" ,"Data": payload},
        )
    
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"message": "An error occurred while processing the payload."},
        )