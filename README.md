# ClawPool Proxy Function

This is the edge proxy that sits between your API client and Anthropic. It's published here so you can see exactly what happens to your requests.

## What this file does

1. **Auth swap** — Your pool key is replaced with a provider's OAuth token
2. **Forward** — The request is sent to `api.anthropic.com` with the swapped credentials
3. **Stream passthrough** — SSE responses are piped through unchanged while token counts are extracted from `message_start` and `message_delta` events
4. **Rate-limit parsing** — Anthropic's `anthropic-ratelimit-unified-*` headers are extracted and passed to a callback
5. **Billing formula** — Cache read tokens are discounted to 10% of face value (matching Anthropic's API pricing); everything else at 100%

Non-streaming responses (like `count_tokens` preflight checks) pass through untouched with no usage tracking.

## What this file does NOT do

All pool management, billing, storage, and auth plumbing live in the private repo. This file has zero imports and never touches KV, Stripe, or any private state.

Private operations are wired in through a `hooks` callback:

```js
hooks: {
  onUsage(tokenIndex, apiKey, usage, isOwnToken, isProviderPoolKey)
  onRateLimits(tokenIndex, limits)
  onRequest(success)
}
```

The hooks show *that* billing and tracking happen, but not *how*.

## Source

This file is synced from the [ClawPool](https://clawpool.ai) private repo. It runs as a Cloudflare Worker at `proxy.clawpool.ai`.
