from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from transformer.controller import Csvgres
from api.router import get_routers

app = FastAPI(title="CSV Database API")
csv_db = Csvgres()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize the CSV database
@app.on_event("startup")
async def startup_event():
    try:
        csv_db.init()
    except Exception as e:
        print(f"Error initializing database: {e}")

# Pydantic model for query requests
class QueryRequest(BaseModel):
    query: str

# Health check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat()
    }

# Query endpoint
@app.post("/query")
async def execute_query(request: QueryRequest):
    try:
        query = request.query.lower()
        
        if "create database" in query:
            await csv_db.create_database(query)
            return {
                "success": True,
                "message": "Database created successfully"
            }
        
        elif any(cmd in query for cmd in [r'\c', r'\connect', 'c ', 'connect ']):
            await csv_db.connect_database(query)
            return {
                "success": True,
                "message": "Database connected successfully"
            }

        elif "create table" in query:
            await csv_db.create_table(query)
            return {
                "success": True,
                "message": "Table created successfully"
            }
        
        
        elif "insert into" in query:
            await csv_db.insert(query)
            return {
                "success": True,
                "message": "Data inserted successfully"
            }
        
        elif "select" in query:
            results = await csv_db.select(query)
            return {
                "success": True,
                "data": results.to_dict(orient='records'),
                "message": "Data selected successfully"
            }
        
        elif "delete from" in query:
            await csv_db.delete_row(query)
            return {
                "success": True,
                "message": "Row deleted successfully"
            }
        
        elif "drop table" in query:
            await csv_db.drop_table(query)
            return {
                "success": True,
                "message": "Table dropped successfully"
            }
        
        elif "drop database" in query:
            await csv_db.drop_database(query)
            return {
                "success": True,
                "message": "Database dropped successfully"
            }
        raise HTTPException(
            status_code=400,
            detail="Only CREATE TABLE, INSERT INTO, and SELECT queries are supported"
        )
    
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=str(error)
        )

# Include all routers from the API
for router in get_routers():
    app.include_router(router)
    
# Add this debug print
for route in app.routes:
    print(f"Registered route: {route.path}")

if __name__ == "__main__":
    import uvicorn
    HOST = "0.0.0.0"
    PORT = 8000
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")