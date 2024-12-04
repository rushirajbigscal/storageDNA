from fastapi import FastAPI , Request

app = FastAPI()

@app.post("/send")
async def print_payload(request: Request):
    # Read the request body (JSON) as a dictionary
    payload = await request.json()
    
    # Print the payload to the console
    print("Request Payload:", payload)
    
    return {"message": "Payload received", "data": payload}