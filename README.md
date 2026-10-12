# v4lens-feeder

Runs every 5 minutes on GitHub Actions for the V4LENS Worker (https://v4lens.pawww.sbs):

1. fills any gap in the Uniswap V4 / Pons event index,
2. computes the Pons launch X-rays waiting in the queue.

Robinhood Chain's public RPCs answer 429 / "busy" to Cloudflare Workers' shared egress IPs, so the
Worker cannot read big log ranges itself. This repo stores nothing; the only secret is `FEED_SECRET`
(same value as the Worker's `ADMIN_SECRET`). If this stops, the Worker keeps indexing by itself in
10-block slices through its dedicated RPC; only X-rays fall behind.

`core.ts` is a copy of `worker/core.ts` in the V4LENS project. Keep them in sync.
