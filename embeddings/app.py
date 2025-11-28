from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

app = FastAPI()
model = SentenceTransformer("intfloat/multilingual-e5-small")  # 384-dim

class EmbedRequest(BaseModel):
  input: list[str]

@app.post("/embed")
def embed(req: EmbedRequest):
  texts = [f"passage: {t}" for t in req.input]
  vecs = model.encode(texts, normalize_embeddings=True).tolist()
  return {"data": vecs, "dim": len(vecs[0]) if vecs else 0, "model": "intfloat/multilingual-e5-small"}