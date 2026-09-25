# social-sentiment-mcp

A small, self-hosted, **personal and non-commercial** tool that gives my AI assistant (Claude)
a read-only view of retail investor sentiment on the handful of stocks I own, so I can manage
my own options positions (covered calls and cash-secured puts). It runs in a single Docker
container on my home NAS and is reachable only by me.

## What it does today
- **Stocktwits** public symbol streams: recent posts, bullish/bearish tags, message rate.
- **ApeWisdom**: aggregate Reddit mention counts and rank per ticker (no Reddit post text).
- Stores only per-ticker daily *numbers* (message rate, % bullish, mention count) in a local
  SQLite file to build a 7-day baseline. No post text or usernames are stored.

## Planned Reddit Data API use (pending approval)
- **Read-only.** Search a few finance subreddits (r/wallstreetbets, r/options, r/stocks,
  r/investing) for posts and top comments mentioning ~10-15 tickers I hold.
- **Low volume.** Roughly one query per ticker per day (a single morning run), well under
  published rate limits; responses cached so repeated questions don't re-query.
- **No writing of any kind**: no posting, commenting, voting, or messaging.
- **No storage of Reddit content.** Post and comment text is fetched on demand, shown to me
  (and summarized by my AI assistant in my private chat), and discarded. Only aggregate counts
  are kept.
- **No usernames retained, no profiling** of Reddit users, no attempt to infer anything about
  individual users.
- **Not used to train any AI or machine-learning model**, and not sold, shared, licensed,
  or used in any commercial product.
- Single dedicated app account, clearly identified User-Agent.

## Running it
Copy `compose.example.yaml` to `compose.yaml`, set `MCP_PATH`, then build and start with
Docker Compose (or Synology Container Manager).

## Tools exposed (MCP)
`ticker_sentiment`, `watchlist_sentiment_scan`, `stocktwits_posts`, `reddit_trending`,
`stocktwits_trending`
