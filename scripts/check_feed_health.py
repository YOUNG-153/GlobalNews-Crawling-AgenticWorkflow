#!/usr/bin/env python3
"""RSS/Sitemap feed health checker for all configured sites.

Sends async HEAD requests to each site's primary RSS/sitemap URL and
reports HTTP status codes.  Results are saved to a dated JSON file.

Usage:
    .venv/bin/python scripts/check_feed_health.py [--project-dir .]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp not installed. Run: pip install aiohttp", file=sys.stderr)
    sys.exit(1)

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed. Run: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


CONCURRENCY = 20
TIMEOUT_SECONDS = 10


def load_feed_urls(project_dir: Path) -> list[dict]:
    """Extract feed URLs from sources.yaml."""
    sources_path = project_dir / "data" / "config" / "sources.yaml"
    with open(sources_path) as f:
        data = yaml.safe_load(f)

    feeds = []
    for site_id, site in data.get("sources", {}).items():
        crawl = site.get("crawl", {})
        method = crawl.get("primary_method", "rss")
        url = crawl.get("rss_url") or crawl.get("sitemap_url")

        if not url:
            feeds.append({
                "site_id": site_id,
                "url": None,
                "method": method,
                "group": site.get("group", "?"),
            })
            continue

        # Resolve relative sitemap URLs
        if url.startswith("/"):
            base = site.get("url", "")
            url = base.rstrip("/") + url

        feeds.append({
            "site_id": site_id,
            "url": url,
            "method": method,
            "group": site.get("group", "?"),
        })

    return feeds


async def check_feed(
    session: aiohttp.ClientSession,
    feed: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Check a single feed URL. Returns result dict."""
    url = feed.get("url")
    if not url:
        return {**feed, "status": "NO_URL", "status_code": None, "error": None}

    async with semaphore:
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                return {
                    **feed,
                    "status": _classify_status(resp.status),
                    "status_code": resp.status,
                    "error": None,
                }
        except asyncio.TimeoutError:
            return {**feed, "status": "TIMEOUT", "status_code": None, "error": "timeout"}
        except aiohttp.ClientError as e:
            return {**feed, "status": "ERROR", "status_code": None, "error": str(e)[:200]}
        except Exception as e:
            return {**feed, "status": "ERROR", "status_code": None, "error": str(e)[:200]}


def _classify_status(code: int) -> str:
    if 200 <= code < 300:
        return "OK"
    if 300 <= code < 400:
        return "REDIRECT"
    if code == 403:
        return "BLOCKED"
    if code in (404, 410):
        return "GONE"
    if code == 406:
        return "NOT_ACCEPTABLE"
    if 500 <= code < 600:
        return "SERVER_ERROR"
    return f"HTTP_{code}"


async def run_checks(feeds: list[dict]) -> list[dict]:
    """Run all feed checks concurrently."""
    semaphore = asyncio.Semaphore(CONCURRENCY)
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; FeedHealthCheck/1.0)",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [check_feed(session, feed, semaphore) for feed in feeds]
        results = await asyncio.gather(*tasks)

    return list(results)


def print_summary(results: list[dict]) -> None:
    """Print human-readable summary to stdout."""
    from collections import Counter

    status_counts = Counter(r["status"] for r in results)

    print(f"\n{'='*70}")
    print(f"  RSS/SITEMAP FEED HEALTH CHECK")
    print(f"  {len(results)} feeds checked at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*70}\n")

    print(f"  {'Status':<20s} {'Count':>6s}")
    print(f"  {'-'*20} {'-'*6}")
    for status, count in status_counts.most_common():
        print(f"  {status:<20s} {count:>6d}")
    print()

    # List problematic feeds
    problems = [r for r in results if r["status"] not in ("OK", "REDIRECT")]
    if problems:
        print(f"  PROBLEMATIC FEEDS ({len(problems)}):")
        print(f"  {'Site':<25s} {'Group':>5s} {'Status':<15s} {'Error'}")
        print(f"  {'-'*25} {'-'*5} {'-'*15} {'-'*30}")
        for r in sorted(problems, key=lambda x: x["site_id"]):
            error = (r.get("error") or "")[:40]
            print(f"  {r['site_id']:<25s} {r['group']:>5s} {r['status']:<15s} {error}")
        print()

    ok_count = status_counts.get("OK", 0) + status_counts.get("REDIRECT", 0)
    print(f"  Health rate: {ok_count}/{len(results)} ({ok_count/len(results)*100:.1f}%)\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="RSS feed health checker")
    parser.add_argument("--project-dir", type=Path, default=Path("."))
    args = parser.parse_args()

    project_dir = args.project_dir.resolve()

    # 1. Load feed URLs
    feeds = load_feed_urls(project_dir)
    print(f"Loaded {len(feeds)} feed URLs from sources.yaml")

    # 2. Run async checks
    results = asyncio.run(run_checks(feeds))

    # 3. Save results
    log_dir = project_dir / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = log_dir / f"feed-health-{date_str}.json"

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Results saved to {output_path}")

    # 4. Print summary
    print_summary(results)


if __name__ == "__main__":
    main()
