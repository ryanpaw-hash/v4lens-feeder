// V4LENS feeder. Runs on GitHub Actions every 5 minutes and does what the Cloudflare Worker cannot,
// because Robinhood Chain's public RPCs refuse Cloudflare's shared egress IPs:
//   1. fills any gap in the Uniswap V4 / Pons event index (POST /api/internal/ingest), and
//   2. computes the launch X-rays waiting in the queue (POST /api/internal/xray).
// It stores nothing. The only secret is FEED_SECRET (same value as the Worker's ADMIN_SECRET).
import { xray } from "./core.ts";

const SITE = process.env.SITE || "https://v4lens.pawww.sbs";
const SECRET = process.env.FEED_SECRET;
const RPCS = (process.env.RPCS || "https://rpc.mainnet.chain.robinhood.com,https://rpc.ordofi.network").split(",");
const BUDGET_MS = Number(process.env.BUDGET_MS || 4.5 * 60_000);
const STEP = 4000;
const PM = "0x8366a39cc670b4001a1121b8f6a443a643e40951", PONS = "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e";
const TOPICS = [[
  "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438", // Initialize
  "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f", // Swap
  "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607", // Pons Launched
]];
if (!SECRET) { console.error("FEED_SECRET is not set"); process.exit(1); }

const t0 = Date.now();
const left = () => BUDGET_MS - (Date.now() - t0);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const hex = (n) => "0x" + n.toString(16);
let id = 0;

async function rpc(method, params) {
  let last;
  for (let round = 0, busy = 0; round < 8; round++) {
    const url = RPCS[(id + round) % RPCS.length];
    try {
      const r = await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: ++id, method, params }), signal: AbortSignal.timeout(45_000) });
      const text = await r.text();
      if (r.status === 429) throw new Error("429 Too Many Requests");
      const j = JSON.parse(text);
      if (j.error) {
        if (/exceeds limit|are allowed/.test(j.error.message)) return { tooMany: true };
        throw new Error(j.error.message);
      }
      return j.result;
    } catch (e) {
      last = e;
      if (/429|Too Many|busy/i.test(String(e.message)) && busy < 6) { busy++; round--; await sleep(1500 * busy); continue; }
      await sleep(600 * (round + 1));
    }
  }
  throw last;
}

async function api(path, init = {}) {
  for (let t = 0; t < 6; t++) {
    try {
      const r = await fetch(SITE + path, { ...init, headers: { ...(init.headers || {}), authorization: `Bearer ${SECRET}`, "content-type": "application/json" }, signal: AbortSignal.timeout(100_000) });
      if (r.ok || r.status === 409) return { status: r.status, body: await r.json().catch(() => ({})) };
      if (r.status === 423) { await sleep(4000); continue; } // an indexer tick holds the lease
      if (r.status === 401) throw new Error("401 unauthorized: FEED_SECRET does not match the Worker's ADMIN_SECRET");
      await sleep(2000 * (t + 1));
    } catch (e) {
      if (/401/.test(String(e))) throw e;
      await sleep(2000 * (t + 1));
    }
  }
  throw new Error("site kept failing: " + path);
}

async function fetchRange(from, to) {
  const logs = await rpc("eth_getLogs", [{ address: [PM, PONS], fromBlock: hex(from), toBlock: hex(to), topics: TOPICS }]);
  if (logs?.tooMany) {
    if (to - from < 50) throw new Error("range too small but still too many logs");
    const m = Math.floor((from + to) / 2);
    return [...(await fetchRange(from, m)), ...(await fetchRange(m + 1, to))];
  }
  return logs;
}

/* ---- 0. can this runner read the chain at all? ---- */
const head = parseInt(await rpc("eth_blockNumber", []), 16);
const status = await (await fetch(`${SITE}/api/status`, { signal: AbortSignal.timeout(30_000) })).json();
let cursor = status.index.block;
console.log(JSON.stringify({ rpc: "ok", head, indexed: cursor, behind: head - cursor }));

/* ---- 1. gap fill ---- */
let ingested = 0;
while (head - cursor > 600 && left() > 60_000) {
  const from = cursor + 1;
  const to = Math.min(head - 4, from + STEP - 1);
  const logs = await fetchRange(from, to);
  const res = await api("/api/internal/ingest", { method: "POST", body: JSON.stringify({ from, to, logs }) });
  if (res.status === 409) { cursor = res.body.expected - 1; continue; } // the Worker moved the cursor itself
  cursor = to;
  ingested += logs.length;
  console.log(JSON.stringify({ ingest: to, logs: logs.length, behind: head - to }));
}

/* ---- 2. X-ray queue ---- */
let saved = 0, failed = 0;
while (left() > 45_000) {
  const q = await api("/api/internal/xray-queue?hours=24&limit=60");
  const items = q.body.items ?? [];
  if (!items.length) break;
  const out = [];
  let next = 0;
  await Promise.all(Array.from({ length: 6 }, async () => {
    while (next < items.length && left() > 25_000) {
      const l = items[next++];
      try { out.push({ token: l.token, x: await xray(rpc, l, l.quote || "0x0000000000000000000000000000000000000000") }); }
      catch (e) { failed++; if (failed < 4) console.error("xray failed", l.token, String(e).slice(0, 120)); }
    }
  }));
  if (!out.length) break;
  for (let k = 0; k < out.length; k += 40) await api("/api/internal/xray", { method: "POST", body: JSON.stringify({ items: out.slice(k, k + 40) }) });
  saved += out.length;
}
console.log(JSON.stringify({ done: true, ingestedLogs: ingested, xraySaved: saved, xrayFailed: failed, secs: Math.round((Date.now() - t0) / 1000) }));
