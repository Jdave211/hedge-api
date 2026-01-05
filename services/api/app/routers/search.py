from fastapi import APIRouter
from pydantic import BaseModel
from app.db.supabase import sb
from app.ai.embed import embed_text

router = APIRouter()

class SearchRequest(BaseModel):
    query: str
    limit: int = 5
    markets_per_event: int = 3
    min_similarity: float = 0.5

@router.post("/search")
def search(req: SearchRequest):
    q_emb = embed_text(req.query)

    payload = sb.rpc("search_kalshi_events_with_markets", {
        "query_embedding": q_emb,
        "match_count": req.limit,
        "markets_per_event": req.markets_per_event,
    }).execute().data

    # Supabase returns a single json object in `.data` for this function
    payload = payload or {"results": []}

    # Enforce a minimum similarity threshold (default: 0.5)
    results = payload.get("results") or []
    filtered_results = [
        r for r in results
        if isinstance(r, dict) and (r.get("similarity") is not None) and float(r.get("similarity")) >= req.min_similarity
    ]

    return {
        "query": req.query,
        "results": filtered_results,
    }
