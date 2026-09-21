"""Collect daily aggregate usage figures from Google Analytics 4 and store them as TSV.

GA4 deletes user- and event-level data after the property's retention period.
Aggregate counts are not personal data, so this script copies them, one row per
day per dimension value, into VFB_reporting_results/analytics/ga4/ where they can
be kept indefinitely and used for long-term trends.

Nothing user-level is requested: no user ids, no client ids, no IPs (GA4 never
exposes them). City rows are kept at monthly grain only and dropped below
MIN_CITY_USERS.

Usage:
  python ga4_usage_collect.py                      # last REFRESH_DAYS days (nightly)
  python ga4_usage_collect.py --start 2023-05-01   # backfill
  python ga4_usage_collect.py --tables term,event  # subset

Credentials: GA4_SERVICE_ACCOUNT_KEY_JSON (the key file's content) or
GA4_SERVICE_ACCOUNT_KEY_PATH. Property: GA4_PROPERTY_ID (default 375054915).
"""
import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
from collections import defaultdict

from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "375054915")
OUT_DIR = os.environ.get("GA4_OUT_DIR", "../VFB_reporting_results/analytics/ga4")
API = "https://analyticsdata.googleapis.com/v1beta/properties/%s:runReport" % PROPERTY_ID
SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
REFRESH_DAYS = 4        # GA4 keeps revising the last ~72 h
MIN_CITY_USERS = 5      # monthly city rows below this are not stored
PAGE_ROWS_PER_DAY = 300
PAGE_LIMIT = 250000     # API maximum rows per request

TRAFFIC = ["activeUsers", "newUsers", "sessions", "engagedSessions",
           "screenPageViews", "userEngagementDuration", "eventCount"]
ID_RE = r"(VFB|FBbt|FBgn|FBti|FBtp|FBal|FBcv|GO)[A-Za-z]*_[0-9A-Za-z]{7,8}"
TIMING_RE = re.compile(r"^\d+(\.\d+)?s$")


def flt(field, value, match="EXACT"):
    return {"filter": {"fieldName": field, "stringFilter": {"matchType": match, "value": value}}}


# name -> dims, metrics, optional filter, days per request (smaller where cardinality is high)
DAILY = {
    "traffic": (["date"], TRAFFIC, None, 365),
    "host": (["date", "hostName"], TRAFFIC, None, 92),
    "country": (["date", "country"], TRAFFIC, None, 31),
    "channel": (["date", "sessionDefaultChannelGroup"], ["sessions", "engagedSessions", "userEngagementDuration"], None, 92),
    "source": (["date", "sessionSource", "sessionMedium"], ["sessions", "engagedSessions", "userEngagementDuration"], None, 31),
    "tech": (["date", "deviceCategory", "operatingSystem", "browser"], ["activeUsers", "sessions", "userEngagementDuration"], None, 31),
    "language": (["date", "language"], ["activeUsers", "userEngagementDuration"], None, 31),
    "new_returning": (["date", "newVsReturning"], ["activeUsers", "sessions", "userEngagementDuration"], None, 92),
    "hour": (["date", "hour"], ["activeUsers", "sessions", "eventCount"], None, 92),
    "page": (["date", "hostName", "pagePath"], ["screenPageViews", "activeUsers", "userEngagementDuration"], None, 7),
    "term": (["date", "hostName", "pagePathPlusQueryString"], ["screenPageViews", "activeUsers", "userEngagementDuration"],
             flt("pagePathPlusQueryString", ID_RE, "PARTIAL_REGEXP"), 7),
    "event": (["date", "streamName", "hostName", "eventName"], ["eventCount", "totalUsers"], None, 7),
    "outbound": (["date", "linkDomain"], ["eventCount"], flt("eventName", "click"), 92),
    "download": (["date", "fileName"], ["eventCount"], flt("eventName", "file_download"), 92),
}
MONTHLY = {   # users are not additive across days, so monthly uniques are asked for directly
    "traffic": (["yearMonth"], TRAFFIC),
    "host": (["yearMonth", "hostName"], TRAFFIC),
    "country": (["yearMonth", "country"], TRAFFIC),
    "city": (["yearMonth", "country", "region", "city"], TRAFFIC),
}


def session():
    raw = os.environ.get("GA4_SERVICE_ACCOUNT_KEY_JSON")
    path = os.environ.get("GA4_SERVICE_ACCOUNT_KEY_PATH")
    if raw:
        creds = service_account.Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    elif path:
        creds = service_account.Credentials.from_service_account_file(path, scopes=SCOPES)
    else:
        sys.exit("No GA4 credentials: set GA4_SERVICE_ACCOUNT_KEY_JSON or GA4_SERVICE_ACCOUNT_KEY_PATH")
    return AuthorizedSession(creds)


def run_report(s, start, end, dims, mets, dim_filter=None):
    rows, offset = [], 0
    while True:
        body = {"dateRanges": [{"startDate": start, "endDate": end}],
                "dimensions": [{"name": d} for d in dims], "metrics": [{"name": m} for m in mets],
                "limit": PAGE_LIMIT, "offset": offset, "returnPropertyQuota": True}
        if dim_filter:
            body["dimensionFilter"] = dim_filter
        for attempt in range(5):
            r = s.post(API, json=body, timeout=300)
            if r.status_code == 200:
                break
            if r.status_code in (429, 500, 502, 503):
                time.sleep(30 * (attempt + 1))
                continue
            sys.exit("GA4 API %s: %s" % (r.status_code, r.text[:500]))
        else:
            sys.exit("GA4 API kept failing: %s" % r.text[:500])
        j = r.json()
        for row in j.get("rows", []):
            rows.append([v["value"] for v in row.get("dimensionValues", [])] +
                        [v["value"] for v in row.get("metricValues", [])])
        offset += PAGE_LIMIT
        if offset >= int(j.get("rowCount", 0)):
            quota = j.get("propertyQuota", {}).get("tokensPerDay", {})
            return rows, quota.get("remaining")


def iso(d):   # GA returns YYYYMMDD
    return "%s-%s-%s" % (d[:4], d[4:6], d[6:])


def num(v):
    f = float(v)
    return int(f) if f == int(f) else round(f, 1)


def normalise_event(name):
    """Split timing suffixes off event names such as 'term-load:ok:0.4s'.
    Returns (normalised name, seconds or None)."""
    parts = name.split(":")
    secs = [p for p in parts if TIMING_RE.match(p)]
    if not secs:
        return name, None
    return ":".join(p for p in parts if not TIMING_RE.match(p)), float(secs[0][:-1])


def post_event(rows):
    events, latency = defaultdict(lambda: [0, 0]), defaultdict(int)
    for date, stream, host, name, count, users in rows:
        norm, secs = normalise_event(name)
        e = events[(date, stream, host, norm)]
        e[0] += int(count)
        e[1] = max(e[1], int(users))   # variants overlap, so max is a lower bound on users
        if secs is not None:
            latency[(date, host, norm, secs)] += int(count)
    return ([list(k) + v for k, v in events.items()],
            [list(k) + [v] for k, v in latency.items()])


def post_term(rows):
    id_re, out = re.compile(ID_RE), defaultdict(lambda: [0, 0, 0.0])
    for date, host, path, views, users, dur in rows:
        m = re.search(r"[?&]id=(" + ID_RE + ")", path) or id_re.search(path)
        if not m:          # GA's regex filter is case-insensitive; ours is not
            continue
        term = m.group(m.lastindex and 1 or 0) if "id=" in m.group(0) else m.group(0)
        t = re.search(r"[?&]i=(" + ID_RE + ")", path)
        o = out[(date, host, term, t.group(1) if t else "")]
        o[0] += int(views); o[1] += int(users); o[2] += float(dur)
    return [list(k) + [v[0], v[1], round(v[2], 1)] for k, v in out.items()]


def post_page(rows):
    by_day = defaultdict(list)
    for r in rows:
        by_day[r[0]].append(r)
    out = []
    for day in by_day.values():
        out += sorted(day, key=lambda r: -int(r[3]))[:PAGE_ROWS_PER_DAY]
    return out


def upsert(path, header, rows, key_col, replaced_keys):
    """Replace every stored row whose key (date or month) was re-pulled; keep the rest."""
    old = []
    if os.path.exists(path):
        with open(path, newline="") as f:
            rd = csv.reader(f, delimiter="\t")
            next(rd, None)
            old = [r for r in rd if r[key_col] not in replaced_keys]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    allrows = sorted(old + [[str(c) for c in r] for r in rows])
    with open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(allrows)


def write_daily(name, header, rows, start, end):
    """One file per table per month, so a nightly run only rewrites the current month."""
    by_month = defaultdict(list)
    for r in rows:
        by_month[r[0][:7]].append(r)
    days, d = set(), start
    while d <= end:
        days.add(d.isoformat()); d += dt.timedelta(days=1)
    for month in {x[:7] for x in days}:
        if by_month.get(month) or os.path.exists(os.path.join(OUT_DIR, "daily", name, month + ".tsv")):
            upsert(os.path.join(OUT_DIR, "daily", name, month + ".tsv"), header, by_month.get(month, []), 0, days)


def chunks(start, end, size):
    while start <= end:
        stop = min(end, start + dt.timedelta(days=size - 1))
        yield start, stop
        start = stop + dt.timedelta(days=1)


def main():
    ap = argparse.ArgumentParser()
    yesterday = dt.date.today() - dt.timedelta(days=1)
    ap.add_argument("--start", default=(yesterday - dt.timedelta(days=REFRESH_DAYS - 1)).isoformat())
    ap.add_argument("--end", default=yesterday.isoformat())
    ap.add_argument("--tables", default="")
    a = ap.parse_args()
    start, end = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
    want = set(filter(None, a.tables.split(",")))
    s, quota = session(), None

    for name, (dims, mets, dim_filter, size) in DAILY.items():
        if want and name not in want:
            continue
        rows = []
        for c0, c1 in chunks(start, end, size):
            got, quota = run_report(s, c0.isoformat(), c1.isoformat(), dims, mets, dim_filter)
            rows += got
        rows = [[iso(r[0])] + r[1:] for r in rows]
        if name == "event":
            rows, latency = post_event(rows)
            write_daily("latency", ["date", "hostName", "event", "seconds", "eventCount"], latency, start, end)
            header = ["date", "streamName", "hostName", "event", "eventCount", "usersMin"]
        elif name == "term":
            rows = post_term(rows)
            header = ["date", "hostName", "term", "template", "screenPageViews", "activeUsersSum", "userEngagementDuration"]
        else:
            if name == "page":
                rows = post_page(rows)
            nd = len(dims)
            rows = [r[:nd] + [num(v) for v in r[nd:]] for r in rows]
            header = dims + mets
        write_daily(name, header, rows, start, end)
        print("%-14s %7d rows  (quota tokens left today: %s)" % (name, len(rows), quota))

    m0 = start.replace(day=1)
    for name, (dims, mets) in MONTHLY.items():
        if want and "monthly" not in want and name not in want:
            continue
        rows, months, m = [], set(), m0
        while m <= end:
            nxt = (m.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
            got, quota = run_report(s, m.isoformat(), min(end, nxt - dt.timedelta(days=1)).isoformat(), dims, mets)
            months.add(m.strftime("%Y-%m"))
            rows += got
            m = nxt
        nd = len(dims)
        rows = [[r[0][:4] + "-" + r[0][4:]] + r[1:nd] + [num(v) for v in r[nd:]] for r in rows]
        if name == "city":
            rows = [r for r in rows if r[nd] >= MIN_CITY_USERS]
        upsert(os.path.join(OUT_DIR, "monthly", name + ".tsv"), ["month"] + dims[1:] + mets, rows, 0, months)
        print("monthly %-7s %6d rows  (quota tokens left today: %s)" % (name, len(rows), quota))


if __name__ == "__main__":
    main()
