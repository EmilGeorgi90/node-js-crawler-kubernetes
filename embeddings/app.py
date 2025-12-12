from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import os

app = FastAPI()
DIM = int(os.environ.get("VECTOR_DIM", "384"))

class Req(BaseModel):
    text: str

@app.post("/embed")
def embed(r: Req):
    v = [0.0] * DIM
    i = 0
    for ch in r.text:
        v[i % DIM] += ord(ch) / 255.0
        i += 1
    return {"embedding": v}

@app.get("/health")
def health():
    return {"ok": True, "dim": DIM}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)