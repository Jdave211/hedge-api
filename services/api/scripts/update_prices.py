import os
import time
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

def fetch_active_markets(limit: int = 500):
    # pull seeded markets (active only). increase limit if needed
    res = (
        sb.table("markets")
        .select("id, external_market_id")
        .eq("platform", "kalshi")
        .eq("is_active", True)
        .limit(limit)
        .execute()
    )
    return res.data or []

def fetch_outcomes_for_market_ids(market_ids):
    # get outcome ids for YES/NO for all markets in one query
    res = (
        sb.table("market_outcomes")
        .select("id, market_id, label")
        .in_("market_id", market_ids)
        .execute()
    )
    return res.data or []

def get_market_snapshot(market_ticker: str) -> dict:
    r = requests.get(f"{KALSHI_BASE}/markets/{market_ticker}", timeout=30)
    r.raise_for_status()
    return (r.json() or {}).get("market") or {}

def cents_to_prob(cents):
    # Kalshi uses cent-based pricing for binaries: 0..100
    if cents is None:
        return None
    return float(cents) / 100.0

def main():
    markets = fetch_active_markets()
    if not markets:
        print("No active markets found in DB. Seed events/markets first.")
        return

    market_ids = [m["id"] for m in markets]
    outcomes = fetch_outcomes_for_market_ids(market_ids)

    # Map: (market_id, LABEL) -> outcome_id
    outcome_map = {}
    for o in outcomes:
        label = (o.get("label") or "").strip().upper()
        outcome_map[(o["market_id"], label)] = o["id"]

    inserts = []
    updated = 0
    skipped = 0

    for m in markets:
        market_id = m["id"]
        ticker = m["external_market_id"]

        yes_outcome_id = outcome_map.get((market_id, "YES"))
        no_outcome_id  = outcome_map.get((market_id, "NO"))

        if not yes_outcome_id or not no_outcome_id:
            skipped += 1
            continue

        snap = get_market_snapshot(ticker)

        yes_bid = cents_to_prob(snap.get("yes_bid"))
        yes_ask = cents_to_prob(snap.get("yes_ask"))
        no_bid  = cents_to_prob(snap.get("no_bid"))
        no_ask  = cents_to_prob(snap.get("no_ask"))

        # "price" field: use midpoint when possible, else conservative executable proxy
        def midpoint(b, a):
            if b is None or a is None:
                return None
            return (b + a) / 2.0

        yes_price = midpoint(yes_bid, yes_ask) or yes_ask or yes_bid
        no_price  = midpoint(no_bid, no_ask) or no_ask or no_bid

        liquidity = snap.get("liquidity")  # this is in cents-notional in your sample

        # Insert time-series rows (don’t overwrite)
        inserts.append({
            "outcome_id": yes_outcome_id,
            "price": yes_price if yes_price is not None else 0,
            "bid": yes_bid,
            "ask": yes_ask,
            "liquidity": liquidity,
            "price_json": {"source": "kalshi_market_endpoint", "ticker": ticker}
        })
        inserts.append({
            "outcome_id": no_outcome_id,
            "price": no_price if no_price is not None else 0,
            "bid": no_bid,
            "ask": no_ask,
            "liquidity": liquidity,
            "price_json": {"source": "kalshi_market_endpoint", "ticker": ticker}
        })

        updated += 1
        time.sleep(0.05)  # small throttle

    # Batch insert (Supabase can handle a big list; if you hit limits, chunk it)
    if inserts:
        sb.table("market_prices").insert(inserts).execute()

    print(f"Updated prices for {updated} markets ({len(inserts)} price rows). Skipped {skipped} (missing outcomes).")

if __name__ == "__main__":
    main()
