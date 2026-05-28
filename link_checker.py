#!/usr/bin/env python3
"""
Nora — Spike's broken link checker bot
Crawls spike.sh/blog, checks all links in parallel, and posts
10 posts worth of broken links to Slack each week — rotating through
all affected posts gradually so fixes stay manageable.
"""

import os
import sys
import json
import time
import logging
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

SEED_URLS = [
    "https://spike.sh/blog",
]

# Used only for --quick test runs (no BFS, no state written).
# Keep these as a stable set of older posts likely to always have some external links worth checking.
QUICK_TEST_URLS = [
    "https://spike.sh/blog/devops-engineer-responsibilities-analyzed-29-job-postings-to-find-out",
    "https://spike.sh/blog/sre-role-2021-analysed-30-job-postings",
    "https://spike.sh/blog/tools-and-products-we-use-at-spike-part-1",
    "https://spike.sh/blog/dashboard-redesign-spike-sh",
    "https://spike.sh/blog/how-to-reduce-alert-noise",
]

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
STATE_FILE        = os.path.join(os.path.dirname(__file__), "state.json")
CACHE_FILE        = os.path.join(os.path.dirname(__file__), ".link_cache.json")

REQUEST_TIMEOUT = 10    # seconds per link-check request
CRAWL_TIMEOUT   = 15    # seconds per page crawl
CRAWL_DELAY     = 0.1   # small delay between page fetches
MAX_PAGES       = 600   # safety cap on pages crawled (blog had >300 in testing)
MAX_LINKS       = 3000  # safety cap on links checked
CHECK_WORKERS   = 20    # parallel workers for link checking
BATCH_SIZE      = 10    # posts to report per week
RETRY_CODES     = {429, 503}

# Statuses that genuinely mean a page is gone.
# 401/403 are intentionally excluded — they almost always mean bot-blocking, not a dead link.
BROKEN_STATUSES = {404, 410, 500, 502, 503, 504}

# Links from these domains are always flagged — regardless of HTTP status.
# Notion pages are private by default; readers can't access them.
# Add any other "should never appear in a public blog" domains here.
ALWAYS_FLAG_DOMAINS = {
    "notion.so",
    "notion.site",
}

# Domains skipped entirely — block bots by design, results are always false positives
SKIP_DOMAINS = {
    "linkedin.com",
    "twitter.com",
    "x.com",
    "facebook.com",
    "instagram.com",
}

# URL path prefixes skipped for internal spike.sh links — these are client-side
# rendered routes: the server returns 404 to HTTP requests even though they load
# fine in a browser. Add any new SPA-routed sections here.
SKIP_URL_PATTERNS = {
    "spike.sh": ["/glossary/", "/examples/"],
}

HEADERS = {
    "User-Agent": "Nora/1.0 (broken-link-checker; +https://spike.sh)",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── State (tracks which posts we've already sent to Slack) ───────────────────

def load_cache() -> list[dict] | None:
    """Return cached broken-link results from the last full run, or None."""
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_cache(broken: list[dict]) -> None:
    with open(CACHE_FILE, "w") as f:
        json.dump(broken, f)
    log.info("Results cached to %s (use --cached to skip crawl next time)", CACHE_FILE)


def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"reported_posts": [], "total_cycles": 0}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
    log.info("State saved to %s", STATE_FILE)


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_crawlable(url: str) -> bool:
    parsed = urlparse(url)
    host   = parsed.netloc.lstrip("www.")
    path   = parsed.path
    return host == "spike.sh" and (path == "/blog" or path.startswith("/blog/"))


def normalise(url: str, base: str) -> str | None:
    try:
        full   = urljoin(base, url.strip()).split("#")[0].strip().rstrip("/")
        parsed = urlparse(full)
        if parsed.scheme not in ("http", "https"):
            return None
        if "share=" in parsed.query:
            return None
        return full
    except Exception:
        return None


_local = threading.local()

def get_session() -> requests.Session:
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _local.session = s
    return _local.session


BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

def check_url(url: str) -> tuple[int | None, str]:
    session = get_session()
    for attempt in range(2):
        try:
            resp = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 405:
                resp = session.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, stream=True)
                resp.close()
            if resp.status_code in RETRY_CODES and attempt == 0:
                time.sleep(3)
                continue

            # If we got a 404, retry once with full browser headers before flagging —
            # some sites (Google, Apple, etc.) return 404 to bots but 200 to browsers.
            if resp.status_code == 404 and attempt == 0:
                retry = session.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT,
                                    headers=BROWSER_HEADERS, stream=True)
                retry.close()
                if retry.status_code < 400:
                    return retry.status_code, ""   # false positive — link is fine
                return retry.status_code, ""

            return resp.status_code, ""
        except requests.exceptions.SSLError as e:
            return None, f"SSL error: {e}"
        except requests.exceptions.ConnectionError as e:
            return None, f"Connection error: {e}"
        except requests.exceptions.Timeout:
            return None, "Timeout"
        except Exception as e:
            return None, str(e)
    return None, "Repeated failure"


# ── Crawler ───────────────────────────────────────────────────────────────────

def crawl(seed_urls: list[str], follow_links: bool = True) -> dict[str, list[tuple[str, str]]]:
    """
    Crawl blog pages and return {link_url: [(page_url, anchor_text), ...]}.
    If follow_links=False, only fetches the seed URLs themselves (no BFS).
    """
    queue   = deque(seed_urls)
    visited = set()
    link_sources: dict[str, list[tuple[str, str]]] = defaultdict(list)

    session = requests.Session()
    session.headers.update(HEADERS)

    while queue and len(visited) < MAX_PAGES:
        page_url = queue.popleft()
        if page_url in visited:
            continue
        visited.add(page_url)

        log.info("[crawl %d/%d] %s", len(visited), MAX_PAGES, page_url)

        try:
            resp = session.get(page_url, timeout=CRAWL_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            log.warning("Could not fetch %s: %s", page_url, e)
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup.find_all("a", href=True):
            href = normalise(tag["href"], page_url)
            if not href:
                continue
            anchor = tag.get_text(strip=True) or tag.get("title") or tag.get("aria-label") or ""
            anchor = anchor[:80]

            entry = (page_url, anchor)
            if entry not in link_sources[href]:
                link_sources[href].append(entry)

            if follow_links and is_crawlable(href) and href not in visited:
                queue.append(href)   # deque.append is O(1)

        time.sleep(CRAWL_DELAY)

    log.info("Crawl done — %d pages, %d unique links", len(visited), len(link_sources))
    return dict(link_sources)


# ── Parallel link checker ─────────────────────────────────────────────────────

def check_all_links(link_sources: dict[str, list[str]]) -> list[dict]:
    """Check all links in parallel. Returns broken-link records."""
    links  = list(link_sources.keys())[:MAX_LINKS]
    total  = len(links)
    broken = []
    done   = 0
    lock   = threading.Lock()

    log.info("Checking %d links with %d parallel workers …", total, CHECK_WORKERS)

    def _check(url: str) -> dict | None:
        nonlocal done
        parsed = urlparse(url)
        host   = parsed.netloc.lstrip("www.")
        path   = parsed.path

        try:
            # Always flag these domains — don't even bother checking HTTP status
            if any(host == d or host.endswith("." + d) for d in ALWAYS_FLAG_DOMAINS):
                return {"url": url, "status": None, "error": "Notion link — readers can't access this", "found_on": link_sources[url]}

            # Skip bot-blocking domains entirely
            if any(host == d or host.endswith("." + d) for d in SKIP_DOMAINS):
                return None

            # Skip known client-side-rendered URL patterns per domain
            for domain, patterns in SKIP_URL_PATTERNS.items():
                if host == domain or host.endswith("." + domain):
                    if any(path.startswith(p) for p in patterns):
                        return None

            status, error = check_url(url)
            is_broken = (status is None) or (status in BROKEN_STATUSES)
            if is_broken:
                return {"url": url, "status": status, "error": error, "found_on": link_sources[url]}
            return None
        finally:
            # Always increment — even for skipped/flagged links — so the counter is accurate
            with lock:
                done += 1
                if done % 100 == 0:
                    log.info("  %d/%d links checked …", done, total)

    with ThreadPoolExecutor(max_workers=CHECK_WORKERS) as pool:
        futures = {pool.submit(_check, url): url for url in links}
        for future in as_completed(futures):
            result = future.result()
            if result:
                broken.append(result)
                log.warning("BROKEN [%s] %s", result["status"] or result["error"], result["url"])

    log.info("Link check done — %d broken out of %d", len(broken), total)
    return broken


# ── Batching logic ────────────────────────────────────────────────────────────

def pick_batch(broken: list[dict], state: dict) -> tuple[dict[str, list], bool]:
    """
    Group broken links by blog post, skip posts we've already reported,
    and return the next BATCH_SIZE posts.

    Returns (batch_dict, did_reset) where batch_dict is:
      {page_url: [(broken_url, status_label), ...]}
    and did_reset=True if we cycled back to the start.
    """
    # Group all broken links by page
    # by_page: page_url -> [(broken_url, status_label, anchor_text), ...]
    by_page: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for item in broken:
        if item["status"]:
            label = str(item["status"])
        elif item["error"].startswith("SSL"):
            label = "SSL error"
        elif item["error"].startswith("Notion"):
            label = "⚠️ Notion link"
        elif item["error"].startswith("Timeout"):
            label = "Timeout"
        elif item["error"].startswith("Connection"):
            label = "Connection error"
        else:
            label = item["error"][:40]
        for page_url, anchor in item["found_on"]:
            # Only include actual blog posts (not category/pagination pages)
            parsed   = urlparse(page_url)
            path     = parsed.path.rstrip("/")
            segments = [s for s in path.split("/") if s]
            # len == 2 already rules out /blog/category/foo/page/2 (4+ segments)
            # and pagination like /blog?query-1-page=2 (1 segment, query string)
            is_post = (len(segments) == 2 and segments[0] == "blog")
            if is_post:
                by_page[page_url].append((item["url"], label, anchor))

    already_reported = set(state.get("reported_posts", []))
    did_reset = False

    # Posts with broken links not yet reported this cycle
    unreported = [p for p in by_page if p not in already_reported]

    # If we've exhausted all broken posts, reset and start fresh
    if not unreported:
        log.info("All broken posts have been reported — resetting for a new cycle.")
        state["reported_posts"] = []
        state["total_cycles"]   = state.get("total_cycles", 0) + 1
        already_reported = set()
        unreported = list(by_page.keys())
        did_reset = True

    # Sort worst-first, take up to BATCH_SIZE
    batch_posts = sorted(unreported, key=lambda p: -len(by_page[p]))[:BATCH_SIZE]
    batch = {p: by_page[p] for p in batch_posts}

    # Mark these as reported
    state["reported_posts"] = list(already_reported | set(batch_posts))
    state["last_run"]        = str(date.today())
    state["remaining_posts"] = len(unreported) - len(batch_posts)

    return batch, did_reset


# ── Slack notification ────────────────────────────────────────────────────────

def send_slack_alert(batch: dict[str, list], state: dict, total_broken_posts: int, did_reset: bool) -> None:
    """Post this week's batch of 10 posts to Slack."""
    batch_count     = len(batch)
    remaining       = state.get("remaining_posts", 0)
    total_links     = sum(len(v) for v in batch.values())
    cycle           = state.get("total_cycles", 0) + 1

    emoji = "🔴" if total_links > 15 else "🟠"

    reset_note = " _(new cycle started — all posts re-queued)_" if did_reset else ""
    queue_note = f"{remaining} more post{'s' if remaining != 1 else ''} queued for next week{'s' if remaining != 1 else ''}"

    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set — printing results instead.")
        print(f"\n{emoji} Week's batch: {batch_count} posts, {total_links} broken links  |  {queue_note}{reset_note}\n")
        for page_url, link_items in batch.items():
            print(f"\n📄 {page_url}")
            for url, label, anchor in link_items:
                anchor_display = f'"{anchor}"' if anchor else "(no anchor text)"
                print(f"   • [{label}] {anchor_display} → {url}")
        return

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} Weekly link fix batch — {batch_count} posts, {total_links} broken links",
            },
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"{queue_note} • cycle #{cycle}{reset_note}"}],
        },
        {"type": "divider"},
    ]

    for page_url, link_items in batch.items():
        lines = []
        for broken_url, status_label, anchor in link_items[:8]:
            anchor_display = f'"{anchor}"' if anchor else "_no anchor text_"
            lines.append(f"• `{status_label}` {anchor_display} → <{broken_url}|{broken_url}>")
        if len(link_items) > 8:
            lines.append(f"_…and {len(link_items) - 8} more on this post_")

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*📄 <{page_url}|{page_url}>*\n" + "\n".join(lines),
            },
        })
        blocks.append({"type": "divider"})

    if len(blocks) > 48:
        blocks = blocks[:48]
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "_Message truncated._"}],
        })

    payload = {
        "text": f"{emoji} {batch_count} blog posts with broken links this week — {queue_note}",
        "blocks": blocks,
    }

    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    if resp.status_code == 200:
        log.info("Slack alert sent.")
    else:
        log.error("Slack error %d: %s", resp.status_code, resp.text)
        sys.exit(1)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    use_cache = "--cached" in sys.argv
    quick     = "--quick"  in sys.argv   # crawl seed URLs only, no BFS, no state written

    mode = " (cached)" if use_cache else " (quick)" if quick else ""
    log.info("=== Spike Blog Link Checker%s ===", mode)

    state = load_state()
    log.info("State loaded — %d posts already reported this cycle",
             len(state.get("reported_posts", [])))

    if use_cache:
        broken = load_cache()
        if broken is None:
            log.error("No cache found — run without --cached first.")
            sys.exit(1)
        # Apply skip filters to cached results too
        def should_skip(url: str) -> bool:
            parsed = urlparse(url)
            host   = parsed.netloc.lstrip("www.")
            path   = parsed.path
            # Never skip always-flag domains
            if any(host == d or host.endswith("." + d) for d in ALWAYS_FLAG_DOMAINS):
                return False
            if any(host == d or host.endswith("." + d) for d in SKIP_DOMAINS):
                return True
            for domain, patterns in SKIP_URL_PATTERNS.items():
                if host == domain or host.endswith("." + domain):
                    if any(path.startswith(p) for p in patterns):
                        return True
            return False

        broken = [item for item in broken if not should_skip(item["url"])]
        log.info("Using cached results: %d broken links (after skip-domain filter)", len(broken))
    else:
        seeds = QUICK_TEST_URLS if quick else SEED_URLS
        link_sources = crawl(seeds, follow_links=not quick)
        if not link_sources:
            log.error("No links found — aborting.")
            sys.exit(1)
        broken = check_all_links(link_sources)
        if not quick:
            save_cache(broken)

    if not broken:
        log.info("✅ No broken links found!")
        return

    batch, did_reset = pick_batch(broken, state)

    test_run = quick or use_cache   # neither test mode should touch state.json

    if not batch:
        log.info("No new broken posts to report this week.")
        if not test_run:
            save_state(state)
        return

    total_broken_posts = len({page_url for item in broken for page_url, _ in item["found_on"]})
    send_slack_alert(batch, state, total_broken_posts, did_reset)

    if not test_run:
        save_state(state)
    else:
        log.info("Test mode (%s): state not saved", "--quick" if quick else "--cached")

    sys.exit(1)  # marks GitHub Actions run as notable — easy to see in the UI


if __name__ == "__main__":
    main()
