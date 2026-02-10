# ClawPool Proxy Function

The deployed proxy function running on `proxy.clawpool.ai`.

## Quick start

```bash
export ANTHROPIC_BASE_URL=https://proxy.clawpool.ai
export ANTHROPIC_AUTH_TOKEN=sk-ant-cpk-...   # your pool key
claude
```

## What this code does

Read `proxy.js` top to bottom — it's the full lifecycle of a proxied request:

1. **Authenticate** — Your pool key is looked up in KV to find your consumer record
2. **Enforce limits** — Check token allowance, attempt auto refresh if exhausted
3. **Resolve token** — Pick a provider's OAuth token (prefer your own, fall back to pool)
4. **Forward** — Auth swap (your pool key for the provider's token) and send to `api.anthropic.com`
5. **Retry on error** — On 401/403/429, try a fallback token
6. **Stream back** — SSE responses are piped through unchanged while token counts are extracted
7. **Bill** — `deductCredits`, `recordUsage`, `updatePoolUsage` run async after the response streams

Nothing is stored. Nothing is logged. The proxy is a passthrough — your request goes in, Claude's response comes back, and token counts are extracted from the SSE stream for billing.
