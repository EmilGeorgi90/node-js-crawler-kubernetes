from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import numpy as np
import hashlib
import os

app = FastAPI()

VECTOR_DIM = int(os.environ.get("VECTOR_DIM", "384"))

class EmbedReq(BaseModel):
    input: List[str]

class EmbedResp(BaseModel):
    data: List[List[float]]

def hash_vec(text: str, dim: int) -> np.ndarray:
    # Deterministic pseudo-embedding using hashing (no external models)
    h = hashlib.sha256(text.encode("utf-8")).digest()
    # Use chunks of the hash as seeds to fill vector
    rng = np.random.default_rng(int.from_bytes(h[:8], "little", signed=False))
    v = rng.normal(0, 1, size=(dim,)).astype(np.float32)
    # L2 normalize to keep scale reasonable
    n = np.linalg.norm(v) + 1e-8
    return (v / n)

@app.post("/embed", response_model=EmbedResp)
def embed(req: EmbedReq):
    if not isinstance(req.input, list):
        return {"data": []}
    out = []
    for s in req.input:
        s = (s or "")[:4000]
        v = hash_vec(s, VECTOR_DIM)
        out.append(v.tolist())
    return {"data": out}
