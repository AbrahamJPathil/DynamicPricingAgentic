"""
Supabase client for the event/festivity pricing agent.

Used ALONGSIDE kafka_publisher.py, not instead of it - build_output_node in
agent.py publishes every proposal to both the event-agent Kafka topic AND
the event_proposals table here, independently.

Provides:
  1. fetch_products()   - replaces the old CSV read in load_catalog_node;
                           pulls SKU rows straight from the `products_sku`
                           table.
  2. insert_proposal()  - inserts one row into `event_proposals` per
                           proposal. Deliberately NOT named publish_proposal
                           (that name belongs to kafka_publisher.py) so both
                           can be imported into agent.py side by side without
                           a name collision.

Required env vars (add both to the project-root .env - NEVER commit this
file or paste real key values into chat/source control):
    SUPABASE_URL
    SUPABASE_KEY   - use the SERVICE ROLE key, not the anon key, unless
                     you've set up an RLS policy on products_sku (SELECT)
                     and event_proposals (INSERT) that explicitly allows
                     the anon role to do both. The service role key bypasses
                     RLS entirely - treat it like a database password.

Table expected for output (run once in the Supabase SQL editor):

    create table public.event_proposals (
      id bigserial not null,
      sku text not null,
      action text not null,
      modifier numeric(6, 4) not null,
      confidence numeric(4, 2) not null,
      rationale text not null,
      created_at timestamp with time zone null default now(),
      constraint event_proposals_pkey primary key (id),
      constraint event_proposals_action_check check (action in ('DISCOUNT', 'HOLD', 'SURCHARGE')),
      constraint event_proposals_confidence_check check (confidence >= 0 and confidence <= 1)
    ) TABLESPACE pg_default;

`modifier` is a signed fractional delta (e.g. 0.0857 for an 8.57% surcharge,
-0.05 for a 5% discount, 0.0 for hold) - always comfortably under 1 for any
realistic price change, matching agent.py's determine_decision_node.
"""

import os
from typing import List, Optional

from dotenv import load_dotenv
from supabase import create_client, Client

# This module's own os.getenv() calls below run the moment agent.py does
# `from supabase_client import ...` - which happens at import time, BEFORE
# agent.py's main() ever calls load_dotenv() itself. Without this line,
# these env vars would always read as None on the very first run of any
# process, regardless of whether .env actually has them - not because the
# values are wrong, but because nothing has loaded the .env file into the
# process yet at this point. Calling load_dotenv() here makes this module
# self-sufficient, independent of import order elsewhere.
load_dotenv()

_SUPABASE_URL = os.getenv("SUPABASE_URL")
_SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not _SUPABASE_URL or not _SUPABASE_KEY:
    raise EnvironmentError(
        "SUPABASE_URL and/or SUPABASE_KEY not found in environment. "
        "Add both to your .env file at the project root:\n"
        "  SUPABASE_URL=https://your-project.supabase.co\n"
        "  SUPABASE_KEY=your_service_role_key_here"
    )

_PRODUCTS_TABLE = os.getenv("SUPABASE_PRODUCTS_TABLE", "products_sku")
_PROPOSALS_TABLE = os.getenv("SUPABASE_PROPOSALS_TABLE", "event_proposals")

_client: Client = create_client(_SUPABASE_URL, _SUPABASE_KEY)


def fetch_products() -> List[dict]:
    """
    Replaces the old CSV read. Pulls every row from products_sku, selecting
    only the columns this agent actually uses - category-level festival/
    holiday matching doesn't need stock, price, or expiry data, that's the
    inventory agent's concern, not this one's.

    Note: Supabase's default page size caps a single .select() at 1000 rows.
    If products_sku ever grows past that, this will silently return only the
    first 1000 - switch to .range()-based pagination at that point.
    """
    response = _client.table(_PRODUCTS_TABLE).select("sku_id, product_name, category").execute()
    rows = response.data or []
    if not rows:
        print(f"[supabase] [WARNING]  '{_PRODUCTS_TABLE}' query returned 0 rows - "
              f"check table contents and RLS policy (SELECT) for the key in use")
    return rows


def insert_proposal(payload: dict, key: Optional[str] = None) -> None:
    """
    Takes the same concise external-contract payload also published to
    Kafka - {agent_id, sku, recommendation: {action, suggested_modifier,
    confidence}, rationale} - flattens it into one row matching the
    event_proposals schema, and inserts it. `key` is accepted (and ignored
    here) only so the call signature lines up with kafka_publisher's
    publish_proposal(payload, key=...) for symmetry in agent.py.
    """
    row = {
        "sku": payload["sku"],
        "action": payload["recommendation"]["action"],
        "modifier": payload["recommendation"]["suggested_modifier"],
        "confidence": payload["recommendation"]["confidence"],
        "rationale": payload["rationale"],
    }
    response = _client.table(_PROPOSALS_TABLE).insert(row).execute()
    # supabase-py raises on outright transport/auth errors but a row
    # rejected by an RLS policy can come back as a 200 with empty data -
    # check explicitly so a silently-dropped insert doesn't look like success.
    if not response.data:
        print(f"[supabase] [WARNING]  insert for sku={payload['sku']} into "
              f"'{_PROPOSALS_TABLE}' returned no data - check RLS policy / key")
    else:
        print(f"[supabase] inserted -> table={_PROPOSALS_TABLE}  sku={payload['sku']}")