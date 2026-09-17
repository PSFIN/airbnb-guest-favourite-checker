from __future__ import annotations

import asyncio
import base64
import io
import json
import random
import re
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import BackgroundTasks, FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

app = FastAPI()

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).parent
CSV_PATH        = BASE_DIR.parent / "Airbnb Guest Favorite Check.csv"
CHECKPOINT_FILE = BASE_DIR / "checkpoint.json"
RUNS_DIR        = BASE_DIR / "runs"
RUNS_DIR.mkdir(exist_ok=True)

# ── In-memory job store ────────────────────────────────────────────────────
# Pub/sub broadcast rather than a single-consumer queue: anyone who opens
# the shared link while a check is already running joins the same live
# stream (replayed from history, then live) instead of starting a second,
# conflicting run against the same checkpoint file.
job_history: dict[str, list[dict]] = {}
job_subscribers: dict[str, list[asyncio.Queue]] = {}
stop_requested: set[str] = set()      # job_ids flagged for manual stop
active_job: dict | None = None        # the one run allowed at a time; None when idle


async def publish(job_id: str, msg: dict):
    job_history.setdefault(job_id, []).append(msg)
    for q in job_subscribers.get(job_id, []):
        await q.put(msg)


def subscribe(job_id: str) -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    job_subscribers.setdefault(job_id, []).append(q)
    return q


def unsubscribe(job_id: str, q: asyncio.Queue):
    subs = job_subscribers.get(job_id)
    if subs and q in subs:
        subs.remove(q)

# ── Parallelism ────────────────────────────────────────────────────────────
WORKERS = 3                            # concurrent browser pages

# Resources we don't need — blocking them speeds up each page load
BLOCK_TYPES = {"image", "media", "font"}


# ── URL / text helpers ─────────────────────────────────────────────────────

def normalize_url(url: str) -> str:
    if not isinstance(url, str):
        return ""
    url = url.strip()
    if not url or url.lower() in ("nan", "none", "-", "n/a"):
        return ""
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    return url


def has_guest_favorite_text(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(re.search(p, t) for p in [
        r"guest favourite", r"guest favorite",
        r"guest-favourite", r"guest-favorite",
        r"one of the most loved homes on airbnb",
    ])


def extract_rating_and_reviews(text: str) -> tuple:
    if not text:
        return ("N/A", "N/A")

    # ── Only search the listing portion of the page ───────────────────────────
    # Airbnb pages always show the listing's own rating/reviews near the top,
    # BEFORE sections like "Meet your host", "Similar stays", etc.
    # Those later sections contain host-aggregate stats or other listings'
    # ratings that would produce false positives, so we cut them off.
    cutoff = re.search(
        r'Meet\s+your\s+host|Similar\s+stays|More\s+places\s+to\s+stay'
        r'|You\s+might\s+also\s+like|Other\s+things\s+to\s+note',
        text, re.IGNORECASE,
    )
    search_text = text[:cutoff.start()] if cutoff else text

    # Strip any remaining host-sentence noise in the listing portion
    noise = [
        r'[Tt]his host has[^\n.]*?reviews[^\n.]*[.\n]',
        r'\d[\d,]*\s+reviews?\s+(?:for|across)\s+(?:other|all)[^\n.]*[.\n]?',
        r'[Hh]ost(?:\'s)?\s+\d[\d,]*\s+reviews?[^\n.]*[.\n]?',
    ]
    cleaned = search_text
    for p in noise:
        cleaned = re.sub(p, ' ', cleaned, flags=re.IGNORECASE)

    RATING = r'([1-4]\.[0-9]{1,2}|5\.0)'
    REVIEWS = r'([\d,]+)'
    SEP = r'[\s·•\-–—]*'

    # ── Try to extract rating + reviews as a pair ─────────────────────────────
    # A rating is only valid when it appears *together* with a review count.
    # This prevents stray decimals (prices, distances, bed counts…) being
    # mistaken for a listing rating.
    paired = [
        # "4.64 · 201 reviews"  /  "4.64 • 6 reviews"
        rf'{RATING}\s*[·•\-–—]\s*{REVIEWS}\s+[Rr]eviews?',
        # "4.64 (201 reviews)"  /  "4.64 (201)"
        rf'{RATING}\s*\(\s*{REVIEWS}(?:\s+[Rr]eviews?)?\s*\)',
        # stacked: "4.64\n201 reviews"
        rf'{RATING}\s*\n+\s*{REVIEWS}\s+[Rr]eviews?',
        # reverse stacked: "201 reviews\n4.64"
        rf'{REVIEWS}\s+[Rr]eviews?\s*\n+\s*{RATING}',
        # "Rated 4.64 out of 5"  +  nearby review count on same stretch
        rf'[Rr]ated\s+{RATING}\s+out\s+of\s+5[^.]*?{REVIEWS}\s+[Rr]eviews?',
    ]

    for pat in paired:
        m = re.search(pat, cleaned, re.IGNORECASE)
        if m:
            g = m.groups()
            # Determine which group is rating vs reviews based on pattern order
            if re.match(r'^[Rr]eviews', pat[:8]):          # reverse stacked
                return (g[1], g[0].replace(",", ""))
            elif 'Rated' in pat:
                return (g[0], g[1].replace(",", ""))
            else:
                return (g[0], g[1].replace(",", ""))

    # ── No paired match → reviews-only fallback (rating stays N/A) ───────────
    m_rev = re.search(rf'{REVIEWS}\s+[Rr]eviews?', cleaned, re.IGNORECASE)
    if m_rev:
        return ("N/A", m_rev.group(1).replace(",", ""))

    return ("N/A", "N/A")


# ── Scraper ────────────────────────────────────────────────────────────────

BLOCKERS = [
    "captcha", "verify you are", "access denied",
    "something went wrong", "temporarily unavailable", "log in or sign up",
]


async def _check_once(page, url: str) -> dict:
    url = normalize_url(url)
    if not url:
        return {"result": "Not Listed", "rating": "N/A", "reviews": "N/A"}

    try:
        # Small random jitter so concurrent workers don't hit Airbnb in lockstep —
        # bursts of near-simultaneous requests (esp. to newly-listed, low-traffic
        # rooms) are what trigger Airbnb's "Something went wrong" soft-block.
        await asyncio.sleep(random.uniform(0, 1.5))
        # "domcontentloaded" fires as soon as the DOM is parsed, well before
        # "load" (which waits on every subresource) — we only need page text,
        # and the networkidle wait + fixed delay below cover hydration anyway.
        # Shorter timeout (20s vs 45s) means a hung/blocked request fails over
        # to retry much sooner instead of stalling the whole run.
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass  # best-effort — proceed even if still loading
        await page.wait_for_timeout(3000)

        # Dismiss cookie / modal if present
        for label in ["Accept", "Accept all", "I agree", "Got it", "OK", "Close"]:
            try:
                loc = page.get_by_role("button", name=label)
                if await loc.count() > 0:
                    await loc.first.click(timeout=2000)
                    await page.wait_for_timeout(500)
                    break
            except Exception:
                pass

        # ── First pass: check without any scrolling ────────────────────────
        full_text = await page.locator("body").inner_text(timeout=8000)
        lowered = (full_text or "").lower()

        if any(x in lowered for x in BLOCKERS):
            return {"result": "Check", "rating": "N/A", "reviews": "N/A"}

        if has_guest_favorite_text(full_text):
            rating, reviews = extract_rating_and_reviews(full_text)
            return {"result": "Yes", "rating": rating, "reviews": reviews}

        # ── Fallback: scroll to surface lazy content, then check again ─────
        for _ in range(3):                          # was 4 × 1200 ms = 4.8 s
            await page.mouse.wheel(0, 2000)
            await page.wait_for_timeout(600)        # was 1200 ms

        full_text = await page.locator("body").inner_text(timeout=8000)
        lowered = (full_text or "").lower()

        if any(x in lowered for x in BLOCKERS):
            return {"result": "Check", "rating": "N/A", "reviews": "N/A"}

        guest_fav = "Yes" if has_guest_favorite_text(full_text) else "No"
        rating, reviews = extract_rating_and_reviews(full_text)
        return {"result": guest_fav, "rating": rating, "reviews": reviews}

    except PlaywrightTimeoutError:
        return {"result": "Check", "rating": "N/A", "reviews": "N/A"}
    except Exception:
        return {"result": "Check", "rating": "N/A", "reviews": "N/A"}


RETRY_DELAYS = [3, 8, 20, 45]   # seconds — backs off further apart on each retry


async def check_listing_async(page, url: str) -> dict:
    """Up to len(RETRY_DELAYS) retries for 'Check' results, with growing backoff."""
    attempts = len(RETRY_DELAYS) + 1
    for attempt in range(attempts):
        result = await _check_once(page, url)
        if result["result"] != "Check":
            return result
        if attempt < attempts - 1:
            # _check_once() re-navigates with page.goto() on its next call,
            # so we just wait here — no need to reload first.
            delay = RETRY_DELAYS[attempt] + random.uniform(0, 2)
            await asyncio.sleep(delay)
    return result


# ── Checkpoint helpers ─────────────────────────────────────────────────────

def save_checkpoint(job_id: str, csv_b64: str, filename: str,
                    total: int, results: list, completed_indices: list):
    CHECKPOINT_FILE.write_text(json.dumps({
        "job_id": job_id,
        "filename": filename,
        "csv_b64": csv_b64,
        "total": total,
        "results": results,
        "completed_indices": completed_indices,   # replaces old next_index
        "saved_at": datetime.now().isoformat(),
    }))


def load_checkpoint() -> dict | None:
    if CHECKPOINT_FILE.exists():
        try:
            return json.loads(CHECKPOINT_FILE.read_text())
        except Exception:
            pass
    return None


def get_completed_set(ck: dict) -> set:
    """Handles both new (completed_indices) and old (next_index) checkpoint formats."""
    if "completed_indices" in ck:
        return set(ck["completed_indices"])
    return set(range(ck.get("next_index", 0)))


def clear_checkpoint():
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


# ── Run history helpers ────────────────────────────────────────────────────

def save_run(filename: str, results: list, duration_ms: int = None):
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    (RUNS_DIR / f"run_{ts}.json").write_text(json.dumps({
        "run_date": datetime.now().isoformat(),
        "filename": filename,
        "results": results,
        "duration_ms": duration_ms,
    }))


def get_all_runs() -> list[dict]:
    runs = sorted(RUNS_DIR.glob("run_*.json"))
    out = []
    for r in runs:
        try:
            out.append(json.loads(r.read_text()))
        except Exception:
            pass
    return out


def get_latest_run() -> dict | None:
    all_runs = get_all_runs()
    return all_runs[-1] if all_runs else None


# ── Analytics ──────────────────────────────────────────────────────────────

def build_summary(results: list) -> list:
    groups: dict = {}
    for r in results:
        g = (r.get("group") or "Unknown").strip()
        if g in ("nan", "None", ""):
            g = "Unknown"
        if g not in groups:
            groups[g] = {"total": 0, "yes": 0, "no": 0, "check": 0,
                         "not_listed": 0, "ratings": [], "reviews": 0}
        d = groups[g]
        d["total"] += 1
        res = r.get("result", "N/A")
        if   res == "Yes":        d["yes"] += 1
        elif res == "No":         d["no"] += 1
        elif res == "Check":      d["check"] += 1
        elif res == "Not Listed": d["not_listed"] += 1
        try:
            d["ratings"].append(float(r["rating"]))
        except (ValueError, TypeError, KeyError):
            pass
        try:
            d["reviews"] += int(r.get("reviews", 0) or 0)
        except (ValueError, TypeError):
            pass

    out = []
    for name, d in sorted(groups.items()):
        listable = d["total"] - d["not_listed"]   # exclude unlisted from % calc
        avg = round(sum(d["ratings"]) / len(d["ratings"]), 2) if d["ratings"] else None
        out.append({
            "group": name,
            "total": d["total"],
            "yes": d["yes"],
            "no": d["no"],
            "check": d["check"],
            "not_listed": d["not_listed"],
            "fav_pct": round(d["yes"] / listable * 100) if listable else 0,
            "avg_rating": avg,
            "total_reviews": d["reviews"],
        })
    return out


def build_comparison(current: list, prev_run: dict) -> dict:
    prev_map = {r["url"]: r for r in prev_run.get("results", [])}
    gained, lost, rating_changes = [], [], []

    for r in current:
        url = r.get("url", "")
        p = prev_map.get(url)
        if not p:
            continue
        if p["result"] != "Yes" and r["result"] == "Yes":
            gained.append({**r, "prev_result": p["result"]})
        elif p["result"] == "Yes" and r["result"] != "Yes":
            lost.append({**r, "prev_result": p["result"]})
        try:
            diff = round(float(r["rating"]) - float(p["rating"]), 2)
            if abs(diff) >= 0.01:
                rating_changes.append({**r, "prev_rating": p["rating"], "diff": diff})
        except (ValueError, TypeError):
            pass

    rating_changes.sort(key=lambda x: abs(x["diff"]), reverse=True)
    return {
        "previous_run_date": prev_run["run_date"],
        "gained": gained,
        "lost": lost,
        "rating_changes": rating_changes[:15],
    }


# ── Background processor ───────────────────────────────────────────────────

async def process_file(
    job_id: str,
    content: bytes,
    filename: str = "uploaded.csv",
    completed_indices: list = None,
    previous_results: list = None,
):
    csv_b64    = base64.b64encode(content).decode()
    prev_res   = list(previous_results or [])
    done_set   = set(completed_indices or [])
    results_dict: dict[int, dict] = {r["index"]: r for r in prev_res}
    ck_lock    = asyncio.Lock()
    run_start  = asyncio.get_event_loop().time()   # track wall time for duration

    try:
        df    = pd.read_csv(io.BytesIO(content))
        total = len(df)

        # Replay already-completed rows instantly
        for r in sorted(prev_res, key=lambda x: x["index"]):
            await publish(job_id, {**r, "type": "result", "total": total,
                             "current": r["index"] + 1, "replayed": True})

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = await browser.new_context(
                viewport={"width": 1440, "height": 2200},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
                locale="en-US",
            )

            # ── Create page pool with resource blocking ────────────────────
            async def make_page():
                pg = await context.new_page()
                await pg.route(
                    "**/*",
                    lambda route: (
                        asyncio.ensure_future(route.abort())
                        if route.request.resource_type in BLOCK_TYPES
                        else asyncio.ensure_future(route.continue_())
                    ),
                )
                return pg

            pages = [await make_page() for _ in range(WORKERS)]

            # Work queue — one item per pending row
            work_q: asyncio.Queue = asyncio.Queue()
            for i, row in df.iterrows():
                if i not in done_set:
                    await work_q.put((int(i), row))

            # ── Worker: pulls rows, checks stop, scrapes ───────────────────
            async def worker(page):
                while True:
                    try:
                        i, row = work_q.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    # Honour stop request before starting new listing
                    if job_id in stop_requested:
                        work_q.task_done()
                        # Drain remaining items so other workers also exit
                        while True:
                            try:
                                work_q.get_nowait()
                                work_q.task_done()
                            except asyncio.QueueEmpty:
                                break
                        break

                    link  = str(row.iloc[2]) if len(row) > 2 else ""
                    name  = str(row.iloc[0]) if len(row) > 0 else ""
                    group = str(row.iloc[1]) if len(row) > 1 else ""

                    # Notify UI that this listing is starting
                    await publish(job_id, {
                        "type": "checking",
                        "current": i + 1,
                        "total": total,
                        "name": name,
                        "group": group,
                        "url": link,
                    })

                    data = await check_listing_async(page, link)
                    row_result = {
                        "index": i, "name": name,
                        "group": group, "url": link, **data,
                    }

                    async with ck_lock:
                        results_dict[i] = row_result
                        done_set.add(i)
                        save_checkpoint(
                            job_id, csv_b64, filename, total,
                            list(results_dict.values()),
                            sorted(done_set),
                        )

                    await publish(job_id, {
                        **row_result,
                        "type": "result",
                        "current": i + 1,
                        "total": total,
                        "replayed": False,
                    })
                    work_q.task_done()

            await asyncio.gather(*[worker(pg) for pg in pages])

            for pg in pages:
                try:
                    await pg.close()
                except Exception:
                    pass
            await browser.close()

        # ── Stopped manually? ──────────────────────────────────────────────
        if job_id in stop_requested:
            stop_requested.discard(job_id)
            await publish(job_id, {"type": "stopped",
                             "current": len(done_set), "total": total})
            return

        # ── Build output CSV (sorted by original row order) ────────────────
        sorted_results = [results_dict[k] for k in sorted(results_dict)]

        while df.shape[1] < 6:
            df[f"Extra_{df.shape[1] + 1}"] = ""
        df.iloc[:, 3] = [r["result"]  for r in sorted_results]
        df.iloc[:, 4] = [r["rating"]  for r in sorted_results]
        df.iloc[:, 5] = [r["reviews"] for r in sorted_results]

        buf = io.StringIO()
        df.to_csv(buf, index=False)
        csv_out = base64.b64encode(buf.getvalue().encode()).decode()

        summary    = build_summary(sorted_results)
        prev_run   = get_latest_run()
        comparison = build_comparison(sorted_results, prev_run) if prev_run else None

        duration_ms = int((asyncio.get_event_loop().time() - run_start) * 1000)
        save_run(filename, sorted_results, duration_ms)
        clear_checkpoint()

        await publish(job_id, {
            "type": "complete",
            "csv": csv_out,
            "summary": summary,
            "comparison": comparison,
        })

    except Exception as e:
        await publish(job_id, {"type": "error", "message": str(e)})

    finally:
        # Release the lock so the next visitor can start a new run — do this
        # regardless of how the job ended (done, stopped, or errored).
        global active_job
        if active_job and active_job.get("job_id") == job_id:
            active_job = None


# ── Routes ─────────────────────────────────────────────────────────────────

@app.get("/last-run")
async def last_run_report():
    all_runs = get_all_runs()
    if not all_runs:
        return JSONResponse({"exists": False})

    run      = all_runs[-1]
    prev_run = all_runs[-2] if len(all_runs) >= 2 else None

    results    = run["results"]
    summary    = build_summary(results)
    comparison = build_comparison(results, prev_run) if prev_run else None

    # Reconstruct a downloadable CSV from the stored results
    buf = io.StringIO()
    buf.write("Property Name,Group,Airbnb Link,Guest Fav,Rating,Reviews\n")
    for r in results:
        def q(v):
            v = str(v) if v is not None else ""
            return f'"{v}"' if ("," in v or '"' in v) else v
        buf.write(f"{q(r.get('name',''))},{q(r.get('group',''))},{q(r.get('url',''))},"
                  f"{q(r.get('result',''))},{q(r.get('rating',''))},{q(r.get('reviews',''))}\n")
    csv_b64 = base64.b64encode(buf.getvalue().encode()).decode()

    return {
        "exists": True,
        "run_date": run["run_date"],
        "filename": run["filename"],
        "duration_ms": run.get("duration_ms"),
        "results": results,
        "summary": summary,
        "comparison": comparison,
        "csv": csv_b64,
    }


@app.get("/existing-file")
async def existing_file():
    path = CSV_PATH.resolve()
    if path.is_file():
        return {"exists": True, "filename": path.name}
    return {"exists": False}


@app.get("/checkpoint")
async def checkpoint_status():
    ck = load_checkpoint()
    if not ck:
        return {"exists": False}
    done = len(get_completed_set(ck))
    return {
        "exists": True,
        "filename": ck["filename"],
        "processed": done,
        "total": ck["total"],
        "saved_at": ck["saved_at"],
    }


@app.get("/active-job")
async def active_job_status():
    """Lets a freshly-loaded page detect a run already in progress (started
    by someone else with the shared link) and join it instead of showing
    the upload screen."""
    if active_job is None:
        return {"active": False}
    return {"active": True, "job_id": active_job["job_id"]}


def _start_job(background_tasks: BackgroundTasks, content: bytes, filename: str,
              completed_indices: list = None, previous_results: list = None) -> dict:
    """Starts a new run, or — if one is already in progress — hands back its
    job_id so the caller joins the same live stream instead of a second,
    conflicting run against the same checkpoint file."""
    global active_job
    if active_job is not None:
        return {"job_id": active_job["job_id"], "joined": True}

    job_id = str(uuid.uuid4())
    active_job = {"job_id": job_id, "filename": filename}
    # A run only starts here, so this is the one place old broadcast state
    # from a previous, now-finished job can be dropped.
    job_history.clear()
    job_subscribers.clear()
    background_tasks.add_task(
        process_file, job_id, content, filename, completed_indices, previous_results,
    )
    return {"job_id": job_id, "joined": False}


@app.post("/upload")
async def upload(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    content = await file.read()
    if active_job is not None:
        return _start_job(background_tasks, content, file.filename)
    clear_checkpoint()
    # Uploaded lists become the shared default for everyone's "Use Existing
    # File" from here on — handy for a team tool with one shared link.
    try:
        CSV_PATH.write_bytes(content)
    except Exception:
        pass
    return _start_job(background_tasks, content, file.filename)


@app.post("/use-existing")
async def use_existing(background_tasks: BackgroundTasks):
    if active_job is not None:
        return _start_job(background_tasks, b"", "")
    path = CSV_PATH.resolve()
    if not path.is_file():
        return JSONResponse({"error": "CSV file not found"}, status_code=404)
    clear_checkpoint()
    content = path.read_bytes()
    return _start_job(background_tasks, content, path.name)


@app.post("/resume")
async def resume(background_tasks: BackgroundTasks):
    if active_job is not None:
        return _start_job(background_tasks, b"", "")
    ck = load_checkpoint()
    if not ck:
        return JSONResponse({"error": "No checkpoint found"}, status_code=404)
    content = base64.b64decode(ck["csv_b64"])
    return _start_job(
        background_tasks, content, ck["filename"],
        sorted(get_completed_set(ck)), ck["results"],
    )


@app.post("/stop/{job_id}")
async def stop_job(job_id: str):
    stop_requested.add(job_id)
    return {"ok": True}


@app.get("/stream/{job_id}")
async def stream(job_id: str):
    async def generate():
        if job_id not in job_history:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Job not found'})}\n\n"
            return

        # Subscribe *before* snapshotting history (both synchronous, no
        # await between them) so nothing published while we're replaying
        # the snapshot can slip through the gap — it'll simply be waiting
        # in our queue by the time we get to it below.
        q = subscribe(job_id)
        snapshot = list(job_history[job_id])

        try:
            # Catch this viewer up on everything that already happened, then
            # switch to live messages — this is what lets a second (or
            # third…) visitor join a run already in progress.
            for msg in snapshot:
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") in ("complete", "error", "stopped"):
                    return

            while True:
                msg = await q.get()
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") in ("complete", "error", "stopped"):
                    break
        finally:
            unsubscribe(job_id, q)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()
