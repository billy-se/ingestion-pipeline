import hashlib
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

app = FastAPI()

class LogRecord(BaseModel):
    id: int
    timestamp: int
    user: str
    action: str
    payload_size: int
    raw_line: str
    checksum: str

@app.post("/ingest")
async def ingest_logs(records: List[LogRecord]):
    valid_records = []
    
    for rec in records:
        computed_hash = hashlib.sha256(rec.raw_line.encode('utf-8')).hexdigest()
        
        if computed_hash != rec.checksum:
            raise HTTPException(status_code=400, detail=f"Checksum mismatch for record ID {rec.id}")
            
        valid_records.append((rec.id, rec.user, rec.action, rec.payload_size))

    print(f"Successfully validated and prepared {len(valid_records)} records for database insertion.")
    
    return {"status": "success", "processed_count": len(valid_records)}

@app.get("/")
async def root():
    return {"message": "Bottleneck Buster API is running!"}