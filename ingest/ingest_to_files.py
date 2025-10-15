# ingest/ingest_to_files.py
import os, json, time, random, requests, sys
from datetime import datetime
from sseclient import SSEClient

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
OUT_DIR = os.environ.get("OUT_DIR", "data/bronze")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 50))
MAX_BACKOFF = 120  # cap
os.makedirs(OUT_DIR, exist_ok=True)

HEADERS = {
    "Accept": "text/event-stream",
    "Cache-Control": "no-cache",
    # Identify your app politely; Wikimedia asks for this:
    "User-Agent": "WikiWatch Student Project (contact: your_email@example.com)"
}

def log(msg):
    print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

def write_batch(buf):
    if not buf: return
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    path = os.path.join(OUT_DIR, f"rc_{ts}.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for row in buf:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    log(f"Wrote {len(buf)} events → {path}")

def connect_and_stream(session):
    # Keep a single long-lived GET connection
    resp = session.get(URL, headers=HEADERS, stream=True, timeout=30)
    if resp.status_code == 429:
        # Honor Retry-After if present; otherwise exponential backoff in main()
        ra = resp.headers.get("Retry-After")
        raise requests.HTTPError(f"429", response=resp) if not ra else time.sleep(int(ra))
    if resp.status_code != 200:
        raise requests.HTTPError(f"{resp.status_code} {resp.reason}", response=resp)

    client = SSEClient(resp)
    buf, last_heartbeat = [], time.time()

    for ev in client.events():
        if ev.event != "message" or not ev.data:
            if time.time() - last_heartbeat > 10:
                log("…alive; waiting for edit events")
                last_heartbeat = time.time()
            continue
        try:
            obj = json.loads(ev.data)
        except Exception:
            continue

        if obj.get("type") in ("edit", "new") and obj.get("namespace", 0) == 0:
            buf.append(obj)
            if len(buf) >= BATCH_SIZE:
                write_batch(buf)
                buf.clear()

    write_batch(buf)  # flush leftovers

def main():
    log(f"Connecting to {URL} … writing batches of {BATCH_SIZE} to {OUT_DIR}")
    session = requests.Session()
    attempt = 0
    while True:
        try:
            connect_and_stream(session)
            attempt = 0  # normal end (rare) → reset
        except KeyboardInterrupt:
            log("Stopping by user.")
            sys.exit(0)
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code == 429:
                # Exponential backoff with jitter
                attempt += 1
                backoff = min(MAX_BACKOFF, (2 ** attempt)) + random.uniform(0, 1.0)
                ra = e.response.headers.get("Retry-After")
                if ra and ra.isdigit():
                    backoff = max(backoff, int(ra))
                log(f"HTTP 429 (rate limited). Reconnecting in {int(backoff)}s …")
                time.sleep(backoff)
            else:
                attempt += 1
                backoff = min(MAX_BACKOFF, (2 ** attempt)) + random.uniform(0, 1.0)
                log(f"HTTP error {code}. Reconnecting in {int(backoff)}s …")
                time.sleep(backoff)
        except (requests.ConnectionError, requests.Timeout) as e:
            attempt += 1
            backoff = min(MAX_BACKOFF, (2 ** attempt)) + random.uniform(0, 1.0)
            log(f"Network issue: {e}. Reconnecting in {int(backoff)}s …")
            time.sleep(backoff)
        except Exception as e:
            attempt += 1
            backoff = min(MAX_BACKOFF, (2 ** attempt)) + random.uniform(0, 1.0)
            log(f"Unexpected error: {e}. Reconnecting in {int(backoff)}s …")
            time.sleep(backoff)

if __name__ == "__main__":
    main()
