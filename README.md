# ClawPool Proxy Function

This is the actual deployed code that handles every API request through `proxy.clawpool.ai`. It's published here so you can see exactly what happens to your requests.

## What this code does

Read `proxy.js` top to bottom — it's the full lifecycle of a proxied request:

1. **Authenticate** — Your pool key is looked up in KV to find your consumer record
2. **Enforce limits** — Check token allowance, attempt auto refresh if exhausted
3. **Resolve token** — Pick a provider's OAuth token (prefer your own, fall back to pool)
4. **Forward** — Auth swap (your pool key for the provider's token) and send to `api.anthropic.com`
5. **Retry on error** — On 401/403/429, try a fallback token
6. **Stream back** — SSE responses are piped through unchanged while token counts are extracted
7. **Bill** — `deductCredits`, `recordUsage`, `updatePoolUsage` run async after the response streams

## What's not here

The imports at the top of `proxy.js` reference private modules that aren't in this repo:

- `auth.js` — Token resolution, rotation, sticky sessions
- `usage.js` — KV counter updates, credit deduction, rate-limit storage
- `ledger.js` — Revenue recording, provider earnings
- `status.js` — Request counters for the status page
- `stripe.js` — Auto refresh payment processing

You can see exactly *which* functions are called and *when*, but not their implementation. The function names tell you what they do — `deductCredits`, `recordUsage`, `updatePoolUsage` — there's nothing hidden.

## Source

This file is synced from the [ClawPool](https://clawpool.ai) private repo via `just publish-proxy`. It runs as a Cloudflare Worker.
