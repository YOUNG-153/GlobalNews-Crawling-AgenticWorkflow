#!/usr/bin/env python3
"""P1 deterministic estimate recalibration script.

Reads ALL crawl data from data/raw/YYYY-MM-DD/all_articles.jsonl and computes
per-site daily article counts. Outputs recommended daily_article_estimate values
based on the median of observed counts (including zero-count days).

LLM judgment: 0%.  Pure arithmetic on observed data.

Usage:
    .venv/bin/python scripts/recalibrate_estimates.py [--project-dir .]
    .venv/bin/python scripts/recalibrate_estimates.py --apply  # Write to sources.yaml
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

# Minimum estimate — protects pipeline.py _daily_est > 0 check
# (pipeline uses estimate * 0.3 as "sufficient collection" threshold;
#  0 disables the check, risking infinite re-queuing)
MIN_ESTIMATE = 5

# Only flag sites where current vs recommended differs by this fraction
CHANGE_THRESHOLD = 0.20  # 20%


def load_sources_config(project_dir: Path) -> dict:
    """Load sources.yaml via YAML parser."""
    import yaml
    sources_path = project_dir / "data" / "config" / "sources.yaml"
    with open(sources_path) as f:
        return yaml.safe_load(f)


def scan_crawl_data(project_dir: Path) -> dict[str, dict[str, int]]:
    """Scan all raw crawl data and return per-date per-site article counts.

    Returns:
        {date_str: {source_id: count, ...}, ...}
    """
    raw_dir = project_dir / "data" / "raw"
    if not raw_dir.exists():
        print(f"ERROR: {raw_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    date_site_counts: dict[str, dict[str, int]] = {}

    for date_dir in sorted(raw_dir.iterdir()):
        if not date_dir.is_dir():
            continue
        date_str = date_dir.name
        jsonl_path = date_dir / "all_articles.jsonl"
        if not jsonl_path.exists():
            continue

        site_counts: dict[str, int] = defaultdict(int)
        try:
            with open(jsonl_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        article = json.loads(line)
                        source_id = article.get("source_id", "unknown")
                        site_counts[source_id] += 1
                    except json.JSONDecodeError:
                        continue
        except OSError as e:
            print(f"WARNING: Could not read {jsonl_path}: {e}", file=sys.stderr)
            continue

        date_site_counts[date_str] = dict(site_counts)

    return date_site_counts


def compute_recommendations(
    sources_config: dict,
    date_site_counts: dict[str, dict[str, int]],
) -> list[dict]:
    """Compute recommended estimates based on median of observed counts.

    For each site, computes the median daily count across ALL crawl dates
    (including dates where the site produced 0 articles).

    Returns:
        List of {site_id, current, median, recommended, change_pct} dicts,
        sorted by absolute change descending.
    """
    all_sites = set(sources_config.get("sources", {}).keys())
    all_dates = sorted(date_site_counts.keys())
    n_dates = len(all_dates)

    if n_dates == 0:
        print("ERROR: No crawl data found", file=sys.stderr)
        sys.exit(1)

    results = []
    for site_id in sorted(all_sites):
        current_est = (
            sources_config["sources"][site_id]
            .get("meta", {})
            .get("daily_article_estimate", 0)
        )

        # Collect daily counts (0 for dates where site produced nothing)
        daily_counts = []
        for date_str in all_dates:
            count = date_site_counts[date_str].get(site_id, 0)
            daily_counts.append(count)

        median_count = statistics.median(daily_counts)
        recommended = max(int(round(median_count)), MIN_ESTIMATE)

        # Calculate change percentage
        if current_est > 0:
            change_pct = abs(recommended - current_est) / current_est
        else:
            change_pct = 1.0 if recommended != current_est else 0.0

        results.append({
            "site_id": site_id,
            "current": current_est,
            "median": round(median_count, 1),
            "recommended": recommended,
            "change_pct": change_pct,
            "daily_counts": daily_counts,
        })

    # Sort by absolute change descending
    results.sort(key=lambda r: abs(r["current"] - r["recommended"]), reverse=True)
    return results


def print_report(results: list[dict], all_dates: list[str]) -> None:
    """Print human-readable recalibration report."""
    print(f"\n{'='*80}")
    print(f"  P1 DETERMINISTIC ESTIMATE RECALIBRATION REPORT")
    print(f"  Dates analyzed: {len(all_dates)} ({all_dates[0]} to {all_dates[-1]})")
    print(f"  MIN_ESTIMATE: {MIN_ESTIMATE} (pipeline.py _daily_est > 0 protection)")
    print(f"  CHANGE_THRESHOLD: {CHANGE_THRESHOLD*100:.0f}%")
    print(f"{'='*80}\n")

    changes = [r for r in results if r["change_pct"] >= CHANGE_THRESHOLD]
    no_changes = [r for r in results if r["change_pct"] < CHANGE_THRESHOLD]

    total_current = sum(r["current"] for r in results)
    total_recommended = sum(r["recommended"] for r in results)

    print(f"  Total current estimate:     {total_current:,}")
    print(f"  Total recommended estimate: {total_recommended:,}")
    print(f"  Delta: {total_recommended - total_current:+,} ({(total_recommended/total_current - 1)*100:+.1f}%)")
    print(f"  Sites needing change (>{CHANGE_THRESHOLD*100:.0f}% diff): {len(changes)}/{len(results)}\n")

    if changes:
        print(f"  {'Site':<25s} {'Current':>8s} {'Median':>8s} {'Recommend':>10s} {'Change':>8s}")
        print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*10} {'-'*8}")
        for r in changes:
            arrow = "↓" if r["recommended"] < r["current"] else "↑"
            print(
                f"  {r['site_id']:<25s} {r['current']:>8d} {r['median']:>8.1f} "
                f"{r['recommended']:>10d} {arrow}{r['change_pct']*100:>6.0f}%"
            )
        print()

    print(f"  Sites unchanged ({len(no_changes)}): estimates within {CHANGE_THRESHOLD*100:.0f}% of observed median.\n")


def apply_to_sources_yaml(project_dir: Path, results: list[dict]) -> int:
    """Apply recommended estimates to sources.yaml using site-block-aware edit.

    P1 HALLUCINATION PREVENTION:
    - Simple regex `daily_article_estimate: 200` is UNSAFE because multiple
      sites can share the same value. The regex matches the FIRST occurrence,
      which may belong to a different site.
    - This implementation tracks the current site block by detecting YAML keys
      at indentation level 2 (e.g., "  chosun:") and only replaces within
      the correct site's block.
    - Preserves comments, blank lines, and string formatting (no yaml.dump).
    """
    import re

    sources_path = project_dir / "data" / "config" / "sources.yaml"
    lines = sources_path.read_text(encoding="utf-8").splitlines(keepends=True)

    # Build change map: {site_id: recommended_value}
    change_map = {}
    for r in results:
        if r["change_pct"] >= CHANGE_THRESHOLD and r["current"] != r["recommended"]:
            change_map[r["site_id"]] = r["recommended"]

    if not change_map:
        print("  No changes needed — all estimates within threshold.")
        return 0

    # Site-block-aware replacement
    # YAML structure: "  site_id:" at indent=2 starts a site block
    site_key_re = re.compile(r"^  (\w[\w_-]*):\s*$")
    estimate_re = re.compile(r"^(\s+daily_article_estimate:\s*)(\d+)(.*)$")

    current_site = None
    applied = 0
    new_lines = []

    for line in lines:
        # Detect site block start
        m = site_key_re.match(line)
        if m:
            current_site = m.group(1)

        # Replace estimate only within the correct site block
        em = estimate_re.match(line)
        if em and current_site in change_map:
            old_val = int(em.group(2))
            new_val = change_map[current_site]
            if old_val != new_val:
                line = f"{em.group(1)}{new_val}{em.group(3)}\n"
                applied += 1
                del change_map[current_site]  # Prevent double-apply

        new_lines.append(line)

    if applied > 0:
        sources_path.write_text("".join(new_lines), encoding="utf-8")
        print(f"  Applied {applied} changes to {sources_path} (site-block-aware, formatting preserved)")
    else:
        print("  No changes applied (values already match).")

    if change_map:
        print(f"  WARNING: {len(change_map)} sites not found in YAML: {list(change_map.keys())[:5]}")

    return applied


def main() -> None:
    parser = argparse.ArgumentParser(description="P1 deterministic estimate recalibration")
    parser.add_argument("--project-dir", type=Path, default=Path("."))
    parser.add_argument("--apply", action="store_true", help="Write changes to sources.yaml")
    args = parser.parse_args()

    project_dir = args.project_dir.resolve()

    # 1. Load config
    config = load_sources_config(project_dir)
    all_sites = config.get("sources", {})
    print(f"Loaded {len(all_sites)} sites from sources.yaml")

    # 2. Scan all crawl data
    date_site_counts = scan_crawl_data(project_dir)
    all_dates = sorted(date_site_counts.keys())
    print(f"Scanned {len(all_dates)} crawl dates")

    # 3. Compute recommendations (pure arithmetic)
    results = compute_recommendations(config, date_site_counts)

    # 4. Print report
    print_report(results, all_dates)

    # 5. Apply if requested
    if args.apply:
        count = apply_to_sources_yaml(project_dir, results)
        print(f"\nDone. {count} sites updated.")
    else:
        print("  Dry run. Use --apply to write changes to sources.yaml.\n")


if __name__ == "__main__":
    main()
