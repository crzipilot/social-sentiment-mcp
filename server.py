"""
social-sentiment-mcp  —  retail sentiment feed for Claude, self-hosted on TheHanger.

Sources (all free, no API keys):
  * Stocktwits public streams  -> recent posts, Bullish/Bearish tags, message rate, watchers
  * ApeWisdom                  -> Reddit mention counts / rank / 24h velocity
                                  (r/wallstreetbets, r/stocks, r/investing, r/options, ...)

Every ticker lookup is also written to a small SQLite file (/data/history.db) so the
server can compare today's chatter against that ticker's own recent baseline.
"""

import os
import sqlite3
import time
from datetime import datetime, timezone
from statistics import mean

import httpx
from mcp.server.fastmcp import FastMCP

MCP_PATH = os.environ.get("MCP_PATH", "/mcp")
DB_PATH = os.environ.get("DB_PATH", "/data/history.db")
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) social-sentiment-mcp/1.0"}
APE_CACHE_SECONDS = 600

mcp = FastMCP(
    "social-sentiment",
    host="0.0.0.0",
    port=8000,
    streamable_http_path=MCP_PATH,
    stateless_http=True,
)

# ---------------------------------------------------------------- storage ----
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def _db():
    con = sqlite3.connect(DB_PATH)
    con.execute(
        """CREATE TABLE IF NOT EXISTS snap(
             symbol TEXT, ts INTEGER, st_msgs_per_hr REAL, st_bull_pct REAL,
             st_watchers INTEGER, ape_mentions INTEGER, ape_rank INTEGER)"""
    )
    return con


def _record(sym, st, ape):
    con = _db()
    # at most one snapshot per symbol per hour, so repeated calls don't skew the baseline
    last = con.execute("SELECT MAX(ts) FROM snap WHERE symbol=?", (sym,)).fetchone()[0]
    now = int(time.time())
    if not last or now - last > 3600:
        con.execute(
            "INSERT INTO snap VALUES (?,?,?,?,?,?,?)",
            (sym, now, st.get("msgs_per_hour"), st.get("bullish_pct"),
             st.get("watchers"), ape.get("mentions"), ape.get("rank")),
        )
        con.commit()
    con.close()


def _baseline(sym, days=7):
    """Averages from snapshots older than 20h and newer than `days` days."""
    now = int(time.time())
    con = _db()
    rows = con.execute(
        "SELECT st_msgs_per_hr, st_bull_pct, ape_mentions, st_watchers FROM snap "
        "WHERE symbol=? AND ts < ? AND ts > ?",
        (sym, now - 20 * 3600, now - days * 86400),
    ).fetchall()
    con.close()
    if not rows:
        return None

    def avg(i):
        v = [r[i] for r in rows if r[i] is not None]
        return round(mean(v), 2) if v else None

    return {"snapshots": len(rows), "msgs_per_hour": avg(0), "bullish_pct": avg(1),
            "reddit_mentions": avg(2), "watchers": avg(3)}


# -------------------------------------------------------------- stocktwits --
def _st_messages(sym, pages=3):
    msgs, info, max_id = [], {}, None
    with httpx.Client(headers=UA, timeout=15) as c:
        for _ in range(pages):
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{sym}.json"
            r = c.get(url, params={"max": max_id} if max_id else None)
            r.raise_for_status()
            d = r.json()
            info = info or d.get("symbol", {})
            batch = d.get("messages", [])
            msgs += batch
            cur = d.get("cursor", {})
            if not batch or not cur.get("more"):
                break
            max_id = cur.get("max")
    return info, msgs


def _st_summary(sym):
    try:
        info, msgs = _st_messages(sym)
    except Exception as e:  # noqa: BLE001
        return {"error": f"stocktwits unavailable: {e}"}
    if not msgs:
        return {"messages": 0, "watchers": info.get("watchlist_count")}
    tags = [(m.get("entities", {}).get("sentiment") or {}).get("basic") for m in msgs]
    bull, bear = tags.count("Bullish"), tags.count("Bearish")
    times = [datetime.fromisoformat(m["created_at"].replace("Z", "+00:00")) for m in msgs]
    span_h = max((max(times) - min(times)).total_seconds() / 3600, 0.25)
    newest_age_h = (datetime.now(timezone.utc) - max(times)).total_seconds() / 3600
    return {
        "messages_sampled": len(msgs),
        "window_hours": round(span_h, 1),
        "msgs_per_hour": round(len(msgs) / span_h, 1),
        "newest_post_hours_ago": round(newest_age_h, 1),
        "bullish": bull,
        "bearish": bear,
        "untagged": len(msgs) - bull - bear,
        "bullish_pct": round(100 * bull / (bull + bear), 1) if bull + bear else None,
        "watchers": info.get("watchlist_count"),
    }


# -------------------------------------------------------------- apewisdom ---
_ape_cache = {"ts": 0, "filter": None, "rows": []}


def _ape_all(filter_="all-stocks"):
    if _ape_cache["filter"] == filter_ and time.time() - _ape_cache["ts"] < APE_CACHE_SECONDS:
        return _ape_cache["rows"]
    rows, page, pages = [], 1, 1
    with httpx.Client(headers=UA, timeout=15) as c:
        while page <= pages and page <= 12:
            d = c.get(f"https://apewisdom.io/api/v1.0/filter/{filter_}/page/{page}").json()
            rows += d.get("results", [])
            pages = d.get("pages", 1)
            page += 1
    _ape_cache.update(ts=time.time(), filter=filter_, rows=rows)
    return rows


def _ape_row(r):
    m, m24 = int(r.get("mentions") or 0), int(r.get("mentions_24h_ago") or 0)
    rank, rank24 = r.get("rank"), r.get("rank_24h_ago")
    return {
        "ticker": r.get("ticker"),
        "name": r.get("name"),
        "rank": rank,
        "rank_24h_ago": rank24,
        "mentions_24h": m,
        "mentions_prev_24h": m24,
        "mention_velocity": round(m / m24, 2) if m24 else None,
        "upvotes": r.get("upvotes"),
    }


def _ape_summary(sym):
    try:
        rows = _ape_all()
    except Exception as e:  # noqa: BLE001
        return {"error": f"apewisdom unavailable: {e}"}
    for r in rows:
        if (r.get("ticker") or "").upper() == sym:
            out = _ape_row(r)
            out["mentions"] = out["mentions_24h"]
            return out
    return {"rank": None, "mentions": 0,
            "note": f"not among the ~{len(rows)} most-mentioned tickers on Reddit — negligible chatter"}


# ----------------------------------------------------------------- flags ----
def _flags(st, ape, base):
    f = []
    v = ape.get("mention_velocity")
    if v and v >= 2 and (ape.get("mentions") or 0) >= 20:
        f.append(f"REDDIT_SPIKE: mentions {v}x the prior 24h")
    r, r24 = ape.get("rank"), ape.get("rank_24h_ago")
    if r and r24 and r <= 50 and r24 - r >= 25:
        f.append(f"RANK_JUMP: Reddit rank {r24} -> {r}")
    bp = st.get("bullish_pct")
    tagged = (st.get("bullish") or 0) + (st.get("bearish") or 0)
    if bp is not None and tagged >= 15:
        if bp >= 80:
            f.append(f"CROWDED_BULL: {bp}% of tagged Stocktwits posts bullish")
        elif bp <= 40:
            f.append(f"BEARISH_TILT: only {bp}% of tagged Stocktwits posts bullish")
    if base and base.get("msgs_per_hour") and st.get("msgs_per_hour"):
        ratio = st["msgs_per_hour"] / base["msgs_per_hour"]
        if ratio >= 2:
            f.append(f"STOCKTWITS_SURGE: message rate {ratio:.1f}x its own recent baseline")
        elif ratio <= 0.4:
            f.append(f"QUIET: message rate {ratio:.1f}x its own recent baseline")
    return f


def _one(sym):
    sym = sym.strip().upper().lstrip("$")
    st, ape = _st_summary(sym), _ape_summary(sym)
    base = _baseline(sym)
    if "error" not in st:
        _record(sym, st, ape if "error" not in ape else {})
    return {"symbol": sym, "stocktwits": st, "reddit": ape,
            "baseline_7d": base or "building — needs snapshots from earlier days",
            "flags": _flags(st, ape, base)}


# ----------------------------------------------------------------- tools ----
@mcp.tool()
def ticker_sentiment(symbol: str) -> dict:
    """Retail social sentiment for one ticker: Stocktwits bullish/bearish split, message
    rate and watcher count, Reddit mention count/rank/24h velocity (ApeWisdom), comparison
    to the ticker's own 7-day baseline, and plain-English flags (REDDIT_SPIKE, RANK_JUMP,
    CROWDED_BULL, BEARISH_TILT, STOCKTWITS_SURGE, QUIET)."""
    return _one(symbol)


@mcp.tool()
def watchlist_sentiment_scan(symbols: str) -> dict:
    """Scan a comma- or space-separated list of tickers (e.g. 'NBIS,ONDS,PLTR') and return
    each one's sentiment summary, with flagged names listed first. Built for the morning
    Options Desk run."""
    syms = [s for s in symbols.replace(",", " ").split() if s][:25]
    results = [_one(s) for s in syms]
    results.sort(key=lambda r: -len(r["flags"]))
    return {"as_of_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "flagged": [r["symbol"] for r in results if r["flags"]],
            "results": results}


@mcp.tool()
def stocktwits_posts(symbol: str, limit: int = 20) -> list:
    """The most recent Stocktwits posts for a ticker (text, time, user's sentiment tag,
    follower count) so Claude can read what retail is actually saying."""
    _, msgs = _st_messages(symbol.strip().upper().lstrip("$"), pages=2)
    out = []
    for m in msgs[: max(1, min(limit, 60))]:
        out.append({
            "time": m.get("created_at"),
            "sentiment": (m.get("entities", {}).get("sentiment") or {}).get("basic"),
            "user_followers": (m.get("user") or {}).get("followers"),
            "likes": (m.get("likes") or {}).get("total", 0),
            "text": (m.get("body") or "")[:400],
        })
    return out


@mcp.tool()
def reddit_trending(top: int = 25, universe: str = "all-stocks") -> list:
    """Most-mentioned tickers on Reddit over the last 24h (ApeWisdom), with rank change and
    mention velocity vs the prior 24h. universe: all-stocks | wallstreetbets | options |
    stocks | investing | SPACs | all-crypto."""
    rows = _ape_all(universe)
    return [_ape_row(r) for r in rows[: max(1, min(top, 100))]]


@mcp.tool()
def stocktwits_trending() -> list:
    """Symbols currently trending on Stocktwits."""
    with httpx.Client(headers=UA, timeout=15) as c:
        d = c.get("https://api.stocktwits.com/api/2/trending/symbols.json").json()
    return [{"symbol": s.get("symbol"), "name": s.get("title"),
             "watchers": s.get("watchlist_count"),
             "trending_score": s.get("trending_score"),
             "summary": (s.get("trends") or {}).get("summary")}
            for s in d.get("symbols", [])]


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
