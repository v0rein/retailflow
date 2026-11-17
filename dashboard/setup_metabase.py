"""Build the RetailFlow Metabase dashboard from code (stdlib only).

Idempotent: everything is looked up by name first, so re-running updates in place.

    MB_ADMIN_EMAIL=... MB_ADMIN_PASSWORD=... python dashboard/setup_metabase.py
"""
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

MB_URL = os.environ.get("MB_URL", "http://localhost:3000")
DB_NAME = "RetailFlow Gold"
DB_FILE = "/data/lakehouse/retailflow.duckdb"  # path inside the Metabase container
DASHBOARD_NAME = "RetailFlow: E-Commerce Analytics"
QUERIES_SQL = Path(__file__).with_name("queries.sql")

BRL = {"number_style": "currency", "currency": "BRL"}
BRL_WHOLE = {**BRL, "decimals": 0}  # cents are noise on chart axes

# Which queries.sql block becomes a card, and how it is drawn.
# Layout uses Metabase's 24-column grid: (row, col, width, height).
CARDS = [
    ("Q1", "line", (0, 0, 14, 8), {
        "graph.dimensions": ["order_year_month"],
        "graph.metrics": ["total_revenue"],
        "graph.x_axis.title_text": "Month",
        "graph.y_axis.title_text": "Revenue (R$)",
        "column_settings": {'["name","total_revenue"]': BRL_WHOLE},
    }),
    ("Q4", "pie", (0, 14, 10, 8), {
        "pie.dimension": "buyer_type",
        "pie.metric": "customer_count",
        "pie.percent_visibility": "legend",
    }),
    ("Q2", "row", (8, 0, 12, 8), {
        "graph.dimensions": ["category"],
        "graph.metrics": ["category_revenue"],
        "graph.y_axis.title_text": "Revenue (R$)",
        "column_settings": {'["name","category_revenue"]': BRL_WHOLE},
    }),
    # Question 3 ranks states, so it uses the by-state query (Q6), not the by-month one (Q3).
    ("Q6", "bar", (8, 12, 12, 8), {
        "graph.dimensions": ["customer_state"],
        "graph.metrics": ["avg_delivery_days"],
        "graph.x_axis.title_text": "Customer state",
        "graph.y_axis.title_text": "Avg delivery days",
    }),
    ("Q5", "table", (16, 0, 24, 11), {
        "column_settings": {
            '["name","seller_revenue"]': BRL,
            '["name","avg_review_score"]': {"decimals": 2},
        },
    }),
]


def load_queries(path=QUERIES_SQL):
    """Parse queries.sql into {"Q1": ("Monthly Revenue Trend", "SELECT ..."), ...}."""
    queries, key = {}, None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("-- Q"):
            key, title = line.removeprefix("-- ").split(":", 1)
            queries[key] = (title.strip(), [])
        elif key:
            queries[key][1].append(line)
    return {k: (title, "\n".join(lines).strip().rstrip(";")) for k, (title, lines) in queries.items()}


def _mask(obj, secret):
    if isinstance(obj, dict):
        return {k: _mask(v, secret) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_mask(v, secret) for v in obj]
    return obj.replace(secret, "***") if secret and isinstance(obj, str) else obj


def error_summary(raw):
    """Error messages from a Metabase error body, safe to print.

    Metabase echoes submitted values, including the password, in "specific-errors"
    (e.g. a 400 from /api/setup), so never print the raw body.
    """
    try:
        data = json.loads(raw)
    except ValueError:
        return "(non-JSON body not shown)"
    if isinstance(data, dict):
        data = data.get("errors") or data.get("message") or sorted(data)
    return json.dumps(_mask(data, os.environ.get("MB_ADMIN_PASSWORD")))


def api(method, path, body=None, session=None):
    req = urllib.request.Request(MB_URL + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None)
    req.add_header("Content-Type", "application/json")
    if session:
        req.add_header("X-Metabase-Session", session)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as err:
        raise SystemExit(f"{method} {path} failed ({err.code}): {error_summary(err.read())}")
    return json.loads(raw) if raw else None


def wait_for_health(timeout=300):
    # Metabase takes a minute or two to boot and run its migrations.
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if api("GET", "/api/health").get("status") == "ok":
                return
        except (urllib.error.URLError, SystemExit, ConnectionError):
            pass
        time.sleep(5)
    raise SystemExit(f"Metabase not healthy at {MB_URL} after {timeout}s")


def login(email, password):
    # The setup token only exists until the first admin is created.
    props = api("GET", "/api/session/properties")
    if not props.get("has-user-setup"):
        api("POST", "/api/setup", {
            "token": props["setup-token"],
            "user": {"email": email, "password": password,
                     "first_name": "RetailFlow", "last_name": "Admin", "site_name": "RetailFlow"},
            "prefs": {"site_name": "RetailFlow", "allow_tracking": False},
        })
        print(f"created admin {email}")
    return api("POST", "/api/session", {"username": email, "password": password})["id"]


def ensure_database(session):
    dbs = api("GET", "/api/database", session=session)
    for db in dbs.get("data", dbs) if isinstance(dbs, dict) else dbs:
        if db["name"] == DB_NAME:
            return db["id"]
    # read_only: the file is mounted :ro, and a writable connection would hold
    # a lock that blocks Airflow's dbt build.
    db = api("POST", "/api/database", {
        "engine": "duckdb", "name": DB_NAME,
        "details": {"database_file": DB_FILE, "read_only": True},
    }, session)
    print(f"created database {DB_NAME} (id {db['id']})")
    return db["id"]


def ensure_cards(session, db_id, queries):
    existing = {c["name"]: c["id"] for c in api("GET", "/api/card", session=session) if not c.get("archived")}
    card_ids = []
    for key, display, _, viz in CARDS:
        title, sql = queries[key]
        payload = {
            "name": title, "display": display, "visualization_settings": viz,
            "dataset_query": {"type": "native", "database": db_id, "native": {"query": sql}},
        }
        if title in existing:
            # Update in place so edits to queries.sql reach the dashboard.
            api("PUT", f"/api/card/{existing[title]}", payload, session)
            card_ids.append(existing[title])
        else:
            card_ids.append(api("POST", "/api/card", payload, session)["id"])
            print(f"created card {title}")
    return card_ids


def ensure_dashboard(session, card_ids):
    found = [d for d in api("GET", "/api/dashboard", session=session) if d["name"] == DASHBOARD_NAME]
    if found:
        dash_id = found[0]["id"]
    else:
        dash_id = api("POST", "/api/dashboard", {"name": DASHBOARD_NAME}, session)["id"]
        print(f"created dashboard {DASHBOARD_NAME}")
    # PUT replaces the whole layout; negative ids mean "new dashcard".
    dashcards = [
        {"id": -i, "card_id": card_id, "row": r, "col": c, "size_x": w, "size_y": h}
        for i, (card_id, (_, _, (r, c, w, h), _)) in enumerate(zip(card_ids, CARDS), start=1)
    ]
    api("PUT", f"/api/dashboard/{dash_id}", {"dashcards": dashcards}, session)
    return dash_id


def main():
    email, password = os.environ.get("MB_ADMIN_EMAIL"), os.environ.get("MB_ADMIN_PASSWORD")
    if not email or not password:
        raise SystemExit("Set MB_ADMIN_EMAIL and MB_ADMIN_PASSWORD first.")
    wait_for_health()
    session = login(email, password)
    db_id = ensure_database(session)
    card_ids = ensure_cards(session, db_id, load_queries())
    dash_id = ensure_dashboard(session, card_ids)
    print(f"Dashboard ready: {MB_URL}/dashboard/{dash_id}")


if __name__ == "__main__":
    main()
