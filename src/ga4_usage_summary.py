"""Build usage_report.md from the aggregate GA4 tables written by ga4_usage_collect.py.

Reads only the stored TSVs, never GA itself, so the report keeps working for
periods GA4 has already purged.
"""
import datetime as dt
import glob
import os

import pandas as pd
import requests

OUT_DIR = os.environ.get("GA4_OUT_DIR", "../VFB_reporting_results/analytics/ga4")
REPORT = os.environ.get("GA4_REPORT", "../VFB_reporting_results/usage_report.md")
NEO = "http://pdb.virtualflybrain.org/db/data/transaction/commit"
M = ["activeUsers", "sessions", "screenPageViews", "userEngagementDuration"]


def daily(name):
    files = sorted(glob.glob(os.path.join(OUT_DIR, "daily", name, "*.tsv")))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_csv(f, sep="\t", keep_default_na=False) for f in files), ignore_index=True)


def monthly(name):
    return pd.read_csv(os.path.join(OUT_DIR, "monthly", name + ".tsv"), sep="\t", keep_default_na=False)


def table(df, cols=None, n=None):
    if cols:
        df = df[cols]
    if n:
        df = df.head(n)
    out = ["| " + " | ".join(str(c) for c in df.columns) + " |", "|" + "---|" * len(df.columns)]
    for row in df.itertuples(index=False):
        out.append("| " + " | ".join(("{:,}".format(v) if isinstance(v, int) else
                                      "{:,.1f}".format(v) if isinstance(v, float) else str(v)).replace("|", "/")
                                     for v in row) + " |")
    return "\n".join(out) + "\n"


def add_rates(df):
    df = df.copy()
    df["engaged hours"] = (df["userEngagementDuration"] / 3600).round(1)
    df["sec/session"] = (df["userEngagementDuration"] / df["sessions"].clip(lower=1)).round(1)
    return df


def chart(title, x, series, ylab):
    lines = ["```mermaid", "xychart-beta", '    title "%s"' % title,
             "    x-axis [%s]" % ", ".join('"%s"' % v for v in x), '    y-axis "%s"' % ylab]
    lines += ["    line [%s]" % ", ".join(str(v) for v in s) for s in series]
    return "\n".join(lines + ["```", ""])


def labels(ids):
    """Term labels from the public VFB knowledge graph (read-only). Missing labels are left blank."""
    try:
        q = {"statements": [{"statement": "MATCH (n:Entity) WHERE n.short_form IN $ids RETURN n.short_form, n.label",
                             "parameters": {"ids": list(ids)}}]}
        r = requests.post(NEO, json=q, auth=("neo4j", "vfb"), timeout=60).json()
        return {row["row"][0]: row["row"][1] for row in r["results"][0]["data"]}
    except Exception as e:   # the report is still useful with bare ids
        print("label lookup failed:", e)
        return {}


def site(host):
    if host in ("", "v2.virtualflybrain.org", "www.v2.virtualflybrain.org"):
        return "v2 viewer"
    if host in ("www.virtualflybrain.org", "virtualflybrain.org", "blog.virtualflybrain.org"):
        return "www / docs"
    if "catmaid" in host:
        return "CATMAID"
    if "chat." in host:
        return "VFBchat"
    if host.endswith("virtualflybrain.org"):
        return "other VFB services"
    if host.endswith(".inf.ed.ac.uk"):
        return "legacy Edinburgh hosts"
    return "other / mirrors"


def top_terms(term, since, n=25):
    t = term[term.date >= since] if since else term
    g = t.groupby("term")[["screenPageViews", "userEngagementDuration"]].sum()
    g["days seen"] = t.groupby("term").date.nunique()
    g = g.sort_values("screenPageViews", ascending=False).head(n).reset_index()
    lab = labels(g.term)
    g["label"] = g.term.map(lab).fillna("")
    g["engaged hours"] = (g.userEngagementDuration / 3600).round(1)
    g = g.rename(columns={"screenPageViews": "views"})
    return table(g, ["term", "label", "views", "engaged hours", "days seen"])


def main():
    today = dt.date.today()
    y1 = (today - dt.timedelta(days=365)).isoformat()
    d30 = (today - dt.timedelta(days=30)).isoformat()
    m12 = y1[:7]
    out = []
    w = out.append

    traffic, mt = daily("traffic"), monthly("traffic")
    w("# VFB usage report\n")
    w("Generated %s from aggregate Google Analytics 4 counts for property 375054915, "
      "covering %s to %s. Source tables: [`analytics/ga4/`](analytics/ga4/). "
      "Only summed counts are stored; nothing here identifies a user.\n" % (today, traffic.date.min(), traffic.date.max()))
    w("**Reading the numbers.** A large and growing share of 'users' and 'sessions' is automated traffic "
      "that loads one page and leaves within a second. Engaged hours (time with the page in the foreground) "
      "is barely touched by that traffic and is the most reliable measure of real use; "
      "seconds per session shows how bot-heavy a row is.\n")

    w("## Yearly totals\n")
    traffic["year"] = traffic.date.str[:4]
    y = traffic.groupby("year")[["sessions", "screenPageViews", "userEngagementDuration", "eventCount"]].sum().reset_index()
    mt["year"] = mt.month.str[:4]
    y["peak monthly users"] = y.year.map(mt.groupby("year").activeUsers.max())
    w(table(add_rates(y), ["year", "peak monthly users", "sessions", "screenPageViews", "engaged hours", "sec/session"]))

    w("## Monthly trend\n")
    m = add_rates(mt)
    w(chart("Engaged hours per month", m.month.tolist(), [m["engaged hours"].tolist()], "hours"))
    w(chart("Monthly active users", m.month.tolist(), [m.activeUsers.tolist()], "users"))
    w(table(m.tail(24), ["month", "activeUsers", "newUsers", "sessions", "screenPageViews", "engaged hours", "sec/session"]))

    w("## By site, last 12 months\n")
    h = monthly("host"); h = h[h.month >= m12]
    h["site"] = h.hostName.map(site)
    g = add_rates(h.groupby("site")[M[1:]].sum().reset_index()).sort_values("engaged hours", ascending=False)
    w("Page views with no host name are the v2 viewer's in-app term changes and are counted under the viewer.\n")
    w(table(g, ["site", "sessions", "screenPageViews", "engaged hours", "sec/session"]))

    w("## Countries, last 12 months\n")
    c = monthly("country"); c = c[c.month >= m12]
    g = add_rates(c.groupby("country")[M].sum().reset_index()).sort_values("engaged hours", ascending=False)
    g = g.rename(columns={"activeUsers": "users (sum of months)"})
    w("Ranked by engaged hours. %d countries in total.\n" % c.country.nunique())
    w(table(g, ["country", "users (sum of months)", "sessions", "engaged hours", "sec/session"], 30))

    w("## Cities, last 12 months\n")
    w("GA4 has no institution or network dimension, so city is the nearest proxy for where labs are. "
      "Cities with fewer than 5 users in a month are not stored. Data-centre cities "
      "(Ashburn, Council Bluffs, Singapore and similar) show up with near-zero seconds per session.\n")
    ci = monthly("city"); ci = ci[(ci.month >= m12) & (ci.city != "(not set)")]
    g = ci.groupby(["city", "country"])[M].sum()
    g["months seen"] = ci.groupby(["city", "country"]).month.nunique()
    g = add_rates(g.reset_index()).sort_values("engaged hours", ascending=False)
    w(table(g, ["city", "country", "months seen", "sessions", "engaged hours", "sec/session"], 50))

    term = daily("term")
    if len(term):
        w("## Most viewed terms\n")
        w("Taken from the term id in viewer URLs and `/reports/` and `/term/` pages. "
          "The adult brain template leads because it is the default scene, not because people search for it.\n")
        for title, since in (("Last 30 days", d30), ("Last 12 months", y1), ("All time", None)):
            w("### %s\n" % title)
            w(top_terms(term, since))
        tp = term[(term.template != "") & (term.date >= y1)].groupby("template").screenPageViews.sum()
        tp = tp.sort_values(ascending=False).head(10).reset_index().rename(columns={"screenPageViews": "views"})
        tp["label"] = tp.template.map(labels(tp.template)).fillna("")
        w("### Templates in use, last 12 months\n")
        w(table(tp, ["template", "label", "views"]))

    w("## Website and documentation pages, last 12 months\n")
    p = daily("page"); p = p[(p.date >= y1) & p.hostName.isin(["www.virtualflybrain.org", "virtualflybrain.org"])]
    g = p.groupby("pagePath")[["screenPageViews", "userEngagementDuration"]].sum().reset_index()
    g["engaged hours"] = (g.userEngagementDuration / 3600).round(1)
    w(table(g.sort_values("engaged hours", ascending=False), ["pagePath", "screenPageViews", "engaged hours"], 30))

    w("## How people arrive\n")
    ch = daily("channel"); ch["year"] = ch.date.str[:4]
    piv = ch.pivot_table(index="sessionDefaultChannelGroup", columns="year", values="engagedSessions", aggfunc="sum", fill_value=0)
    w("Engaged sessions by channel and year.\n")
    w(table(piv.astype(int).reset_index().rename(columns={"sessionDefaultChannelGroup": "channel"})))
    s = daily("source"); s = s[s.date >= y1]
    g = s.groupby("sessionSource")[["sessions", "userEngagementDuration"]].sum().reset_index()
    g["engaged hours"] = (g.userEngagementDuration / 3600).round(1)
    w("Top sources, last 12 months.\n")
    w(table(g.sort_values("engaged hours", ascending=False), ["sessionSource", "sessions", "engaged hours"], 25))

    ev = daily("event"); ev["month"] = ev.date.str[:7]
    w("## Queries run in the viewer, last 12 months\n")
    q = ev[(ev.date >= y1) & ev.event.str.startswith("query-run:")].copy()
    if len(q):
        q["query"] = q.event.str.split(":").str[2]
        g = q.groupby("query").eventCount.sum().sort_values(ascending=False).head(25).reset_index()
        w(table(g.rename(columns={"eventCount": "runs"})))
    else:
        w("No `query-run` events recorded yet.\n")

    w("## VFBchat and MCP\n")
    g = ev[ev.event.isin(["chat_query", "mcp_tool_call"])].pivot_table(
        index="month", columns="event", values="eventCount", aggfunc="sum", fill_value=0).astype(int).reset_index()
    w(table(g.tail(18)))

    w("## Viewer reliability\n")
    v = ev[ev.hostName == "v2.virtualflybrain.org"]
    names = ["session_start", "disconnected", "reconnected-resumed", "reconnect-failed-reloading", "reconnect-exhausted"]
    g = v[v.event.isin(names)].pivot_table(index="month", columns="event", values="eventCount", aggfunc="sum", fill_value=0)
    g = g.reindex(columns=names, fill_value=0).astype(int)
    g["disconnects per 100 sessions"] = (100 * g.disconnected / g.session_start.clip(lower=1)).round(1)
    w(table(g.reset_index().tail(18)))
    lat = daily("latency")
    if len(lat):
        lat = lat[lat.hostName == "v2.virtualflybrain.org"].copy(); lat["month"] = lat.date.str[:7]
        rows = []
        for (mo, e), d in lat.groupby(["month", "event"]):
            d = d.groupby("seconds").eventCount.sum().sort_index(); cum = d.cumsum() / d.sum()
            if d.sum() >= 100:
                rows.append({"month": mo, "event": e, "n": int(d.sum()),
                             "median s": float(cum[cum >= 0.5].index[0]), "p95 s": float(cum[cum >= 0.95].index[0])})
        if rows:
            keep = ("term-load:ok", "direct-terminfo:ok", "startup-model", "startup-first-term", "direct-geom:obj:ok")
            r = pd.DataFrame(rows); r = r[r.event.isin(keep)]
            w("Load times from the timing suffix on viewer events (months with at least 100 samples).\n")
            w(table(r.sort_values(["event", "month"])))

    w("## Outbound links clicked, last 12 months\n")
    o = daily("outbound"); o = o[(o.date >= y1) & (o.linkDomain != "")]
    w(table(o.groupby("linkDomain").eventCount.sum().sort_values(ascending=False).head(20).reset_index()
            .rename(columns={"eventCount": "clicks"})))

    w("## Devices and browsers, last 12 months\n")
    t = daily("tech"); t = t[t.date >= y1]
    g = t.groupby(["deviceCategory", "browser"])[["sessions", "userEngagementDuration"]].sum().reset_index()
    g["engaged hours"] = (g.userEngagementDuration / 3600).round(1)
    w(table(g.sort_values("engaged hours", ascending=False), ["deviceCategory", "browser", "sessions", "engaged hours"], 12))

    w("## What this data cannot show\n")
    w("- **Institutions.** GA4 dropped the network-domain dimension and never exposes IP addresses. "
      "City is as close as it gets.\n"
      "- **Search terms typed into VFB.** The viewer does not send a `search` event, so `searchTerm` is empty. "
      "Term popularity above comes from term ids in URLs.\n"
      "- **Event detail.** No custom dimensions are registered on the property, so event parameters "
      "(`event_label`, `event_category`, and the VFBchat and MCP parameters such as tool name and topic) "
      "are discarded from reports. Registration is not retroactive.\n"
      "- **Yearly unique users.** Users do not add up across days or months; only daily and monthly uniques are stored.\n")

    with open(REPORT, "w") as f:
        f.write("\n".join(out))
    print("wrote", REPORT)


if __name__ == "__main__":
    main()
