from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import re

app = FastAPI()

PRODUCT_URL_RE = re.compile(r"/[^/]+/(\d+)(?:\?.*)?$", re.IGNORECASE)
PRODUCT_HINTS = ("product", "auction", "lot", "bid", "hammer", "estimate")

class Doc(BaseModel):
    url: str
    title: Optional[str] = ""
    text: Optional[str] = ""
    html: Optional[str] = ""

class DetectRequest(BaseModel):
    docs: List[Doc]

@app.get("/health")
def health():
    return {"ok": True}

def score_doc(d: Doc):
    url = d.url or ""
    title = (d.title or "").lower()
    text = (d.text or "").lower()

    m = PRODUCT_URL_RE.search(url)
    product_id = m.group(1) if m else None
    has_digits = any(ch.isdigit() for ch in url)
    has_hint = any(h in url.lower() for h in PRODUCT_HINTS) or any(h in title for h in PRODUCT_HINTS)

    is_product = bool(product_id or (has_digits and has_hint))
    conf = 0.9 if product_id else (0.75 if (has_digits and has_hint) else 0.3)

    fields: Dict[str, Any] = {
        "productId": product_id,
        "title": (d.title or "")[:256],
        "hasDigitsInUrl": has_digits,
        "hasHints": has_hint,
        "urlLength": len(url),
        "textLength": len(text),
    }
    return {"url": url, "isProduct": is_product, "confidence": conf, "fields": fields}

@app.post("/detect")
def detect(body: DetectRequest):
  if not body or not isinstance(body.docs, list):
      return {"results": []}
  results = [score_doc(doc) for doc in body.docs]
  return {"results": results}
