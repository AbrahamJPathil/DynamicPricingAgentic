import os
import json
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from supabase import create_client, Client
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

# -- Configuration & Setup ---------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise EnvironmentError("Supabase service key missing in .env file")

# Using the service key bypasses RLS for backend writes
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

KAFKA_BROKER = "localhost:9092"
CONSUMER_GROUP = "fastapi_supabase_sink_v3"  # Bumped to v3 to trigger a fresh backfill
TOPICS = ["inventory-agent", "competitor-agent", "final-prices"]

# Global flag to cleanly shut down the background thread
shutdown_event = threading.Event()

# -- Background Kafka Consumer (The Writer) ----------------------------------
def consume_kafka_to_supabase():
    """Runs continuously in a background thread, pushing Kafka data to Supabase."""
    
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest", # Backfills old data instantly
        "enable.auto.commit": True
    })
    
    consumer.subscribe(TOPICS)
    print(f"[Background Task] Kafka Consumer started. Subscribed to {TOPICS}")

    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[Kafka Error] {msg.error()}")
                continue

            try:
                topic = msg.topic()
                payload = json.loads(msg.value().decode("utf-8"))
                sku = payload.get("sku")
                
                if not sku:
                    continue

                # 1. Handle Inventory Updates
                if topic == "inventory-agent":
                    rec = payload["recommendation"]
                    data = {
                        "sku": sku,
                        "action": rec["action"],
                        "modifier": rec["suggested_modifier"],
                        "confidence": rec["confidence"],
                        "rationale": payload["rationale"]
                    }
                    supabase.table("inventory_proposals").insert(data).execute()
                    print(f" [DB Sync] INSERT inventory_proposals -> {sku}")

                # 2. Handle Competitor Updates
                elif topic == "competitor-agent":
                    rec = payload["recommendation"]
                    data = {
                        "sku": sku,
                        "modifier": rec["suggested_modifier"],
                        "confidence": rec["confidence"],
                        "rationale": payload["rationale"]
                    }
                    supabase.table("competitor_proposals").insert(data).execute()
                    print(f" [DB Sync] INSERT competitor_proposals -> {sku}")

                # 3. Handle Final Price Updates
                elif topic == "final-prices":
                    rec = payload["final_recommendation"]
                    data = {
                        "sku": sku,
                        "action": rec["action"],
                        "modifier": rec["suggested_modifier"],
                        "confidence": rec["confidence"],
                        "status": payload["status"],
                        "rationale": payload["rationale"]
                    }
                    supabase.table("final_prices").insert(data).execute()
                    print(f" [DB Sync] INSERT final_prices -> {sku}")

            except Exception as e:
                print(f"[Sync Error] Failed to process message from {msg.topic()}: {e}")

    finally:
        consumer.close()
        print("[Background Task] Kafka Consumer shut down cleanly.")

# -- FastAPI Lifespan Manager ------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Launch the consumer thread
    shutdown_event.clear()
    thread = threading.Thread(target=consume_kafka_to_supabase, daemon=True)
    thread.start()
    
    yield # App is running
    
    # Shutdown: Signal the thread to stop and wait for it
    print("\n[System] Shutting down background tasks...")
    shutdown_event.set()
    thread.join(timeout=3.0)

# -- FastAPI Application & Endpoints (The Reader) ----------------------------
app = FastAPI(
    title="Dynamic Pricing API",
    lifespan=lifespan
)

@app.get("/api/v1/pricing/review-queue")
async def get_review_queue():
    """Fetches the most recent state of SKUs flagged as FALLBACK."""
    try:
        # Fetch all records ordered newest to oldest
        response = supabase.table("final_prices").select("*").order("updated_at", desc=True).execute()
        
        # Deduplicate: Keep only the most recent entry per SKU
        latest_state = {}
        for row in response.data:
            if row["sku"] not in latest_state:
                latest_state[row["sku"]] = row
                
        # Filter the current active state for fallbacks
        fallbacks = [row for row in latest_state.values() if row["status"] == "FALLBACK"]
        
        return {"data": fallbacks, "count": len(fallbacks)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/pricing/sku/{sku_id}")
async def get_sku_details(sku_id: str):
    """Fetches the most recent pricing context for a specific SKU."""
    try:
        final_resp = supabase.table("final_prices").select("*").eq("sku", sku_id).order("updated_at", desc=True).limit(1).execute()
        if not final_resp.data:
            raise HTTPException(status_code=404, detail="SKU not found in final prices")
        
        inv_resp = supabase.table("inventory_proposals").select("*").eq("sku", sku_id).order("updated_at", desc=True).limit(1).execute()
        comp_resp = supabase.table("competitor_proposals").select("*").eq("sku", sku_id).order("updated_at", desc=True).limit(1).execute()

        return {
            "sku": sku_id,
            "final_decision": final_resp.data[0] if final_resp.data else None,
            "upstream_context": {
                "inventory_agent": inv_resp.data[0] if inv_resp.data else None,
                "competitor_agent": comp_resp.data[0] if comp_resp.data else None
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/pricing/metrics")
async def get_metrics():
    """Aggregates system-wide pricing metrics based on the current state."""
    try:
        # Fetch minimal needed data, ordered newest to oldest
        response = supabase.table("final_prices").select("sku, action, status").order("updated_at", desc=True).execute()
        
        # Deduplicate: Keep only the most recent entry per SKU
        latest_state = {}
        for row in response.data:
            if row["sku"] not in latest_state:
                latest_state[row["sku"]] = row
                
        current_data = list(latest_state.values())

        return {
            "overview": {
                "total_skus_tracked": len(current_data),
                "pending_human_reviews": sum(1 for row in current_data if row.get("status") == "FALLBACK")
            },
            "actions_breakdown": {
                "discounts": sum(1 for row in current_data if row.get("action") == "DISCOUNT"),
                "surcharges": sum(1 for row in current_data if row.get("action") == "SURCHARGE"),
                "holds": sum(1 for row in current_data if row.get("action") == "HOLD")
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))