#!/usr/bin/env python3
"""
Pull hourly electricity usage history from Contact Energy NZ and report peaks.

Purpose: find the highest hourly consumption on record to help scope a
backup generator. NOTE: the Contact API's finest resolution is hourly, so a
reading of 6.0 kWh in an hour means an AVERAGE draw of 6.0 kW across that
hour. Instantaneous peaks (oven + dryer + heat pump start-up) will be higher.
Treat the numbers here as a floor, not a ceiling.

Usage:
    pip install contact-energy-nz aiohttp   # the package forgets to pull in aiohttp
    export CONTACT_USERNAME="you@example.com"
    export CONTACT_PASSWORD="..."
    python contact_peak_usage.py --days 365 --csv hourly.csv
"""
import argparse
import asyncio
import csv
import datetime as dt
import os
import statistics
import sys
from collections import defaultdict

from contact_energy_nz import AuthException, ContactEnergyApi

# Hourly data lags by a few days; the library's own tests go back a week.
LAG_DAYS = 3
CONCURRENCY = 4


async def fetch_day(api, day, sem, retries=3):
    """Fetch one day of hourly data, retrying on transient errors."""
    async with sem:
        for attempt in range(1, retries + 1):
            try:
                return await api.get_hourly_usage(day)
            except AuthException:
                raise
            except Exception as err:  # network blips, 5xx, bad JSON
                if attempt == retries:
                    print(f"  ! {day}: giving up ({err})", file=sys.stderr)
                    return []
                await asyncio.sleep(1.5 * attempt)


async def collect(api, days):
    end = dt.date.today() - dt.timedelta(days=LAG_DAYS)
    start = end - dt.timedelta(days=days - 1)
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [
        fetch_day(api, start + dt.timedelta(days=i), sem) for i in range(days)
    ]
    rows = []
    done = 0
    for coro in asyncio.as_completed(tasks):
        rows.extend(await coro)
        done += 1
        if done % 30 == 0 or done == days:
            print(f"  fetched {done}/{days} days", file=sys.stderr)
    return sorted(rows, key=lambda r: r.date)


def summarise(rows, top_n):
    if not rows:
        print("No data returned.")
        return

    values = [r.value for r in rows]
    values_sorted = sorted(values)

    def pct(p):
        idx = min(len(values_sorted) - 1, int(round(p / 100 * (len(values_sorted) - 1))))
        return values_sorted[idx]

    first, last = rows[0].date, rows[-1].date
    print(f"\nRange: {first:%Y-%m-%d} to {last:%Y-%m-%d}  ({len(rows)} hourly readings)")
    print("kWh in one hour == average kW across that hour.\n")

    print("Overall")
    print(f"  Peak hour           {max(values):6.2f} kW avg")
    print(f"  99th percentile     {pct(99):6.2f} kW avg")
    print(f"  95th percentile     {pct(95):6.2f} kW avg")
    print(f"  Median hour         {statistics.median(values):6.2f} kW avg")
    print(f"  Mean hour           {statistics.fmean(values):6.2f} kW avg")

    print(f"\nTop {top_n} hours")
    for r in sorted(rows, key=lambda r: r.value, reverse=True)[:top_n]:
        print(f"  {r.date:%a %Y-%m-%d %H:00}  {r.value:6.2f} kW avg")

    by_month = defaultdict(list)
    for r in rows:
        by_month[(r.date.year, r.date.month)].append(r.value)
    print("\nPeak hour per month")
    for (y, m), vals in sorted(by_month.items()):
        print(f"  {y}-{m:02d}  max {max(vals):6.2f}   p95 {sorted(vals)[int(0.95 * (len(vals) - 1))]:6.2f}")

    by_hour = defaultdict(list)
    for r in rows:
        by_hour[r.date.hour].append(r.value)
    print("\nPeak by time of day (max seen in that hour slot)")
    for h in range(24):
        vals = by_hour.get(h)
        if vals:
            bar = "#" * int(max(vals) * 4)
            print(f"  {h:02d}:00  {max(vals):6.2f}  {bar}")

    # Daily energy helps size fuel/battery, not the generator's kW rating.
    by_day = defaultdict(float)
    for r in rows:
        by_day[r.date.date()] += r.value
    worst_day, worst_kwh = max(by_day.items(), key=lambda kv: kv[1])
    print(f"\nHighest single day: {worst_day} used {worst_kwh:.1f} kWh")


def write_csv(rows, path):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["timestamp", "kwh", "offpeak_kwh", "dollars"])
        for r in rows:
            w.writerow([r.date.isoformat(), r.value, r.offpeak_value, r.dollar_value])
    print(f"\nWrote {len(rows)} rows to {path}")


async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=365, help="how many days of history to pull (default 365)")
    ap.add_argument("--top", type=int, default=20, help="how many peak hours to list")
    ap.add_argument("--csv", help="also write every hourly reading to this CSV file")
    args = ap.parse_args()

    user, pw = os.environ.get("CONTACT_USERNAME"), os.environ.get("CONTACT_PASSWORD")
    if not (user and pw):
        sys.exit("Set CONTACT_USERNAME and CONTACT_PASSWORD in the environment.")

    print("Logging in...", file=sys.stderr)
    try:
        api = await ContactEnergyApi.from_credentials(user, pw)
        await api.account_summary()
    except AuthException as err:
        sys.exit(f"Login failed: {err}")

    print(f"Pulling {args.days} days of hourly data (one request per day)...", file=sys.stderr)
    rows = await collect(api, args.days)
    summarise(rows, args.top)
    if args.csv:
        write_csv(rows, args.csv)


if __name__ == "__main__":
    asyncio.run(main())
