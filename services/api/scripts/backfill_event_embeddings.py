import os
from typing import List, Dict
from dotenv import load_dotenv
from supabase import create_client
from openai import OpenAI

load_dotenv()

sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

EMBED_MODEL = "text-embedding-3-small"  # 1536 dims
BATCH_SIZE = 64

def build_event_text(e: Dict) -> str:
    parts = [
        e.get("title") or "",
        e.get("subtitle") or "",
        e.get("description") or "",
        f"category: {e.get('category') or ''}",
        f"series: {e.get('series_ticker') or ''}",
        f"region: {e.get('region') or ''}",
    ]
    return "\n".join([p.strip() for p in parts if p and p.strip()])

def fetch_events_missing_embeddings(limit: int) -> List[Dict]:
    res = (
        sb.table("kalshi_events")
        .select("id,title,subtitle,description,category,series_ticker,region")
        .is_("embedding", None)
        .limit(limit)
        .execute()
    )
    return res.data or []

def update_event_embedding(event_id: str, embedding: List[float]) -> None:
    # pgvector accepts an array of floats
    sb.table("kalshi_events").update({"embedding": embedding}).eq("id", event_id).execute()

def main():
    total = 0
    while True:
        rows = fetch_events_missing_embeddings(BATCH_SIZE)
        if not rows:
            print("No more events missing embeddings ✅")
            break

        texts = [build_event_text(r) for r in rows]

        # Embed in one batch call
        emb = client.embeddings.create(model=EMBED_MODEL, input=texts)
        vectors = [d.embedding for d in emb.data]

        for r, v in zip(rows, vectors):
            update_event_embedding(r["id"], v)

        total += len(rows)
        print(f"Embedded + updated {len(rows)} events (total={total})")

if __name__ == "__main__":
    main()
