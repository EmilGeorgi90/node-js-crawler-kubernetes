from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import re

app = FastAPI()

class Req(BaseModel):
    url: str | None = None
    text: str | None = None

@app.post("/detect")
def detect(r: Req):
    url = (r.url or "").lower()
    text = (r.text or "")
    is_product = bool(re.search(r"/auktion/", url)) or bool(re.search(r"/(\d+)(?:\?.*)?$", url))
    label = "product" if is_product else "page"
    conf = 0.85 if is_product else 0.6
    return {"label": label, "confidence": conf}

@app.get("/health")
def health():
    return {"ok": True}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)