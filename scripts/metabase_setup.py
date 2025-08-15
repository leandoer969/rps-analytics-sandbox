# scripts/metabase_setup.py
import json
import os
import time
import urllib.request
import urllib.error

# ---- Config via env (with sensible defaults) ----
MB_BASE = os.getenv("MB_BASE", "http://localhost:3000")

# Admin (from your .env)
ADMIN_EMAIL = os.getenv("MB_EMAIL") or os.getenv("MB_ADMIN_EMAIL")
ADMIN_PASS = os.getenv("MB_PW") or os.getenv("MB_PASSWORD")

# Optional user profile bits (nice-to-have)
ADMIN_FIRST = os.getenv("MB_FIRSTNAME", "Admin")
ADMIN_LAST = os.getenv("MB_LASTNAME", "User")

# Warehouse connection (Metabase must reach Postgres *from inside Docker*)
RPS_HOST = os.getenv("POSTGRES_HOST", "postgres")
RPS_DB = os.getenv("POSTGRES_DB", "rps")
RPS_USER = os.getenv("POSTGRES_USER", "rps_user")
RPS_PASS = os.getenv("POSTGRES_PASSWORD", "rps_password")
RPS_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

# Site preferences
SITE_NAME = os.getenv("MB_SITE_NAME", "RPS Analytics")
SITE_LOCALE = os.getenv("MB_SITE_LOCALE", "en")
TRACKING = os.getenv("MB_ALLOW_TRACKING", "false").lower() == "true"

# Connection name inside Metabase
CONN_NAME = os.getenv("MB_CONN_NAME", "RPS Warehouse")


# ---- Small HTTP helpers ----
def _json_headers(extra=None):
    h = {"Content-Type": "application/json"}
    if extra:
        h.update(extra)
    return h


def get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req) as r:
        body = r.read()
        if body:
            return r.getcode(), json.loads(body)
        return r.getcode(), None


def post(url, payload, headers=None):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers=_json_headers(headers), method="POST"
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.getcode(), json.load(r)
    except urllib.error.HTTPError as e:
        msg = e.read().decode()
        print(f"HTTP {e.code} POST {url}\n{msg}")
        raise


def put(url, payload, headers=None):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers=_json_headers(headers), method="PUT"
    )
    try:
        with urllib.request.urlopen(req) as r:
            body = r.read()
            return r.getcode(), json.loads(body) if body else None
    except urllib.error.HTTPError as e:
        msg = e.read().decode()
        print(f"HTTP {e.code} PUT {url}\n{msg}")
        raise


# ---- Bootstrap flow primitives ----
def wait_for_health():
    print("⏳ Waiting for Metabase /api/health…")
    for _ in range(180):
        try:
            code, _ = get(f"{MB_BASE}/api/health")
            if code == 200:
                print("✅ Metabase healthy.")
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("Metabase did not become healthy in time.")


def get_session_properties():
    code, props = get(f"{MB_BASE}/api/session/properties")
    if code != 200:
        raise RuntimeError("Could not fetch /api/session/properties")
    return props


def get_setup_token():
    props = get_session_properties()
    return props.get("setup-token")


def initial_setup(token: str):
    # This payload works on recent Metabase versions (requires `prefs`)
    payload = {
        "token": token,
        "user": {
            "first_name": ADMIN_FIRST,
            "last_name": ADMIN_LAST,
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASS,
        },
        "database": {
            "engine": "postgres",
            "name": CONN_NAME,
            "details": {
                "host": RPS_HOST,
                "port": RPS_PORT,
                "dbname": RPS_DB,
                "user": RPS_USER,
                "password": RPS_PASS,
                "ssl": False,
            },
        },
        # Some Metabase builds expect `settings` while others expect/require `prefs`.
        # We'll provide both. Metabase will ignore unknown keys.
        "settings": {
            "site_name": SITE_NAME,
            "site_locale": SITE_LOCALE,
            "allow_tracking": TRACKING,
        },
        "prefs": {
            "site_name": SITE_NAME,
            "site_locale": SITE_LOCALE,
        },
    }
    print("🚀 Doing initial setup with /api/setup …")
    return post(f"{MB_BASE}/api/setup", payload)


def login_session():
    print("🔐 Logging in via /api/session …")
    code, resp = post(
        f"{MB_BASE}/api/session",
        {"username": ADMIN_EMAIL, "password": ADMIN_PASS},
    )
    if code != 200:
        raise RuntimeError("Login failed.")
    return resp["id"]  # session token


def ensure_site_prefs(session_id: str):
    """Set site prefs in a version-tolerant way."""
    hdr = {"X-Metabase-Session": session_id}

    def safe_put(key: str, value):
        try:
            code, _ = put(f"{MB_BASE}/api/setting/{key}", {"value": value}, headers=hdr)
            print(f"   • set {key} = {value} (HTTP {code})")
        except urllib.error.HTTPError as e:
            # Be tolerant across versions/editions; skip unknown or forbidden keys
            msg = f"(skip {key}: HTTP {e.code})"
            try:
                body = e.read().decode()
                if body:
                    msg = f"(skip {key}: HTTP {e.code} — {body[:140]})"
            except Exception:
                pass
            print("   ", msg)

    print("⚙️  Ensuring site preferences …")
    # Common keys that exist broadly across versions:
    safe_put("site-name", "RPS Analytics")
    safe_put("site-locale", "en")
    # Disable anonymous tracking where the key exists:
    safe_put("anon-tracking-enabled", False)
    # Optional: timezone (tolerate if not present)
    safe_put("report-timezone", "UTC")


def ensure_database(session_id: str):
    hdr = {"X-Metabase-Session": session_id}
    # List existing DB connections
    code, dblist = get(f"{MB_BASE}/api/database", headers=hdr)
    if code == 200 and isinstance(dblist, list):
        for db in dblist:
            if db.get("name") == CONN_NAME:
                print(
                    "ℹ️  Metabase database connection already exists — skipping create."
                )
                return

    payload = {
        "engine": "postgres",
        "name": CONN_NAME,
        "details": {
            "host": RPS_HOST,
            "port": RPS_PORT,
            "dbname": RPS_DB,
            "user": RPS_USER,
            "password": RPS_PASS,
            "ssl": False,
        },
    }
    print("➕ Creating Metabase database connection …")
    post(f"{MB_BASE}/api/database", payload, headers=hdr)
    print("✅ Database connection created.")


def main():
    # Basic sanity
    if not ADMIN_EMAIL or not ADMIN_PASS:
        raise SystemExit(
            "Missing MB_EMAIL and/or MB_PW (or MB_PASSWORD). "
            "Set them in your environment/.env and retry."
        )

    wait_for_health()
    token = get_setup_token()

    if token:
        print(f"🔑 Setup token present: {token}")
        try:
            initial_setup(token)
            print("✅ Initial setup complete.")
            # DO NOT return here — continue to prefs & DB connect
        except urllib.error.HTTPError as e:
            # If someone already clicked through the wizard, Metabase returns 403
            if e.code == 403:
                print(
                    "ℹ️  Setup appears already done (403). Falling back to login flow."
                )
            else:
                raise

    # Idempotent fallback path (or after fresh setup):
    print("ℹ️  Proceeding with login + ensure prefs + ensure warehouse connection …")
    sid = login_session()
    ensure_site_prefs(sid)
    ensure_database(sid)
    print("✅ Metabase is ready.")


if __name__ == "__main__":
    main()
