from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import re, json, os
import numpy as np

app = FastAPI()
model = SentenceTransformer("intfloat/multilingual-e5-small")

WEIGHTS_PATH = os.getenv("LR_WEIGHTS", "/models/lr.json")
if os.path.exists(WEIGHTS_PATH):
    with open(WEIGHTS_PATH, "r") as f:
        LR = json.load(f)
else:
    LR = {"w":[0.0]*384, "b": 0.0}

class InputDoc(BaseModel):
    url: str
    title: str | None = None
    text: str | None = None
    html: str | None = None

class Batch(BaseModel):
    docs: list[InputDoc]

PRICE_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[\.,]\d{3})*(?:[\.,]\d{2})?)\s*(?:kr|dkk|eur|€|usd|\$)", re.I)
ID_RE    = re.compile(r"/([^/]+)/(\d+)(?:\?|$)", re.I)

def sigmoid(x): return 1/(1+np.exp(-x))

def extract_fields(text: str, url: str):
    price = None
    m = PRICE_RE.search(text or "")
    if m: price = m.group(1)
    slug, pid = None, None
    m2 = ID_RE.search(url or "")
    if m2: slug, pid = m2.group(1), m2.group(2)
    title = None
    if text: title = text.strip().split("\n",1)[0][:120]
    return {"price": price, "slug": slug, "productId": pid, "guessedTitle": title}

@app.post("/detect")
def detect(b: Batch):
    inputs = []
    for d in b.docs:
        t = ((d.title or "") + " " + (d.text or "")).strip()
        inputs.append("passage: " + re.sub(r"\s+", " ", t)[:4000])

    vecs = model.encode(inputs, normalize_embeddings=True)
    w = np.array(LR["w"], dtype=np.float32)
    b0 = float(LR.get("b", 0.0))

    outputs = []
    for i, d in enumerate(b.docs):
        v = vecs[i]
        z = float(np.dot(v, w) + b0)
        prob = 1/(1+np.exp(-z))
        fields = extract_fields((d.text or "")[:4000], d.url)
        outputs.append({"url": d.url, "isProduct": prob >= 0.6, "confidence": float(prob), "fields": fields})
    return {"results": outputs, "dim": len(vecs[0]) if len(inputs) else 0, "model": "e5-small+LR"}
