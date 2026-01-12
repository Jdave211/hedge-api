from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.db.supabase import sb
from app.ai.embed import embed_text
import traceback

router = APIRouter()

class SearchRequest(BaseModel):
    query: str
    limit: int = 10
    min_similarity: float = 0.2  # Lower threshold since direct market search

@router.post("/search")
def search(req: SearchRequest):
    try:
        # Validate query
        if not req.query or not req.query.strip():
            raise HTTPException(status_code=400, detail="Query cannot be empty")
        
        # Generate embedding
        try:
            q_emb = embed_text(req.query)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate embedding: {str(e)}"
            )

        # Call Supabase RPC function (match_markets)
        try:
            rpc_response = sb.rpc("match_markets", {
                "query_embedding": q_emb,
                "match_count": req.limit,
            }).execute()
            
            markets = rpc_response.data or []
        except Exception as e:
            error_msg = str(e)
            # Check if it's a database function error
            if "function" in error_msg.lower() or "does not exist" in error_msg.lower():
                raise HTTPException(
                    status_code=500,
                    detail="Database function 'match_markets' not found. Please ensure the database is properly set up."
                )
            raise HTTPException(
                status_code=500,
                detail=f"Database query failed: {error_msg}"
            )

        # Filter by minimum similarity threshold
        filtered_markets = [
            m for m in markets
            if isinstance(m, dict) and (m.get("similarity") is not None) and float(m.get("similarity")) >= req.min_similarity
        ]
        
        # Transform to match expected frontend format
        # Group markets as individual events since they're already the unit we want
        results = []
        for market in filtered_markets:
            # Create a result structure that matches the frontend expectations
            result = {
                "event_id": market.get("id") or market.get("external_id"),
                "event_title": market.get("title", ""),
                "similarity": market.get("similarity", 0),
                "markets": [{
                    "market_id": market.get("id") or market.get("external_id"),
                    "market_title": market.get("title", ""),
                    "external_market_id": market.get("external_id") or market.get("slug"),
                    "outcomes": [
                        {
                            "label": outcome,
                            "outcome_id": f"{market.get('id')}_{outcome.lower()}",
                            "latest_price": {
                                "price": price,
                                "ts": market.get("updated_at"),
                            } if price is not None else None
                        }
                        for outcome, price in zip(
                            market.get("outcomes", []),
                            market.get("outcome_prices", [])
                        )
                    ] if market.get("outcomes") else []
                }]
            }
            results.append(result)

        return {
            "query": req.query,
            "results": results,
        }
    except HTTPException:
        raise
    except Exception as e:
        # Log full traceback for debugging
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
