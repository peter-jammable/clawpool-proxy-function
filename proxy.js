/**
 * ClawPool edge proxy — public, auditable request forwarding.
 *
 * This file is published to a public repository so anyone can verify
 * exactly what happens to their API requests. It contains:
 *   - Auth swap (pool key → provider OAuth token)
 *   - Request forwarding to api.anthropic.com
 *   - SSE stream passthrough with token-count extraction
 *   - Rate-limit header parsing
 *   - Billable token formula (cache reads at 10%)
 *
 * All pool management, billing, and storage happen through the `hooks`
 * callback — this file never touches KV, Stripe, or any private state.
 *
 * Zero imports. Fully self-contained.
 */

const UPSTREAM = "https://api.anthropic.com";

// ── Orchestration ───────────────────────────────────────────────────

/**
 * Swap the pool key for the provider's OAuth token and forward to Anthropic.
 *
 * Callers are responsible for any header mutations (e.g. beta flags)
 * before passing the request here.
 *
 * @param {Request}     originalRequest - incoming request (headers are cloned, not mutated)
 * @param {URL}         url             - parsed request URL (pathname + search are forwarded)
 * @param {string}      resolvedToken   - provider OAuth token (sk-ant-oat-…)
 * @param {ArrayBuffer} bodyBytes       - pre-read request body (or null)
 * @returns {Promise<Response>}
 */
export function proxyRequest(originalRequest, url, resolvedToken, bodyBytes) {
  const headers = new Headers(originalRequest.headers);

  // Auth swap: replace consumer's pool key with provider's OAuth token
  headers.delete("x-api-key");
  headers.set("Authorization", `Bearer ${resolvedToken}`);
  headers.set("Host", "api.anthropic.com");

  const upstreamUrl = UPSTREAM + url.pathname + url.search;

  return fetch(upstreamUrl, {
    method: originalRequest.method,
    headers,
    body: bodyBytes,
  });
}

/**
 * Process an Anthropic response: extract rate limits, track streaming
 * usage via hooks, and pipe the body back to the caller.
 *
 * Non-streaming responses (e.g. count_tokens preflight) pass through
 * untouched — only SSE streams get usage tracking.
 *
 * hooks: {
 *   onUsage(tokenIndex, apiKey, usage, isOwnToken, isProviderPoolKey)
 *   onRateLimits(tokenIndex, limits)
 *   onRequest(success)
 * }
 *
 * All hooks are optional. Each receives just enough context to do its
 * job — the proxy never sees how they're implemented.
 */
export function handleResponse(response, tokenIndex, apiKey, hooks, ctx, isOwnToken = false, isProviderPoolKey = false) {
  if (hooks.onRequest) {
    ctx.waitUntil(Promise.resolve(hooks.onRequest(response.ok)));
  }

  // Capture rate-limit headers from every response (including errors)
  const rateLimits = extractRateLimits(response);
  if (rateLimits && hooks.onRateLimits) {
    ctx.waitUntil(Promise.resolve(hooks.onRateLimits(tokenIndex, rateLimits)));
  }

  if (!response.ok) {
    return new Response(response.body, {
      status: response.status,
      headers: response.headers,
    });
  }

  // Non-streaming (e.g. count_tokens preflight) — pass through untouched
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("text/event-stream")) {
    return new Response(response.body, {
      status: response.status,
      headers: response.headers,
    });
  }

  // Streaming: pipe through usage-tracking transform
  let usageResolve;
  const usagePromise = new Promise((resolve) => {
    usageResolve = resolve;
  });

  const trackingStream = createUsageTrackingStream((usage) => {
    usageResolve(usage);
  });

  const trackedBody = response.body.pipeThrough(trackingStream);

  if (hooks.onUsage) {
    ctx.waitUntil(
      usagePromise.then((usage) =>
        hooks.onUsage(tokenIndex, apiKey, usage, isOwnToken, isProviderPoolKey)
      )
    );
  }

  return new Response(trackedBody, {
    status: response.status,
    headers: response.headers,
  });
}

// ── Helpers ──────────────────────────────────────────────────────────

/**
 * Billable tokens from a usage object.
 * Cache reads are discounted to 10% of face value (matching Anthropic's API pricing).
 * Cache creation tokens are counted at full value.
 */
export function totalTokens(usage) {
  return (usage.input_tokens || 0)
    + (usage.output_tokens || 0)
    + Math.round((usage.cache_read_tokens || 0) * 0.1)
    + (usage.cache_creation_tokens || 0);
}

/**
 * Extract Anthropic's unified rate-limit headers from a response.
 * Returns an object with parsed values, or null if no headers are present.
 */
export function extractRateLimits(response) {
  const limits = {};

  const fiveHourUtilization = response.headers.get("anthropic-ratelimit-unified-5h-utilization");
  const fiveHourReset = response.headers.get("anthropic-ratelimit-unified-5h-reset");
  const fiveHourStatus = response.headers.get("anthropic-ratelimit-unified-5h-status");
  const sevenDayUtilization = response.headers.get("anthropic-ratelimit-unified-7d-utilization");
  const sevenDayReset = response.headers.get("anthropic-ratelimit-unified-7d-reset");
  const sevenDayStatus = response.headers.get("anthropic-ratelimit-unified-7d-status");
  const unifiedStatus = response.headers.get("anthropic-ratelimit-unified-status");
  const unifiedReset = response.headers.get("anthropic-ratelimit-unified-reset");
  const representativeClaim = response.headers.get("anthropic-ratelimit-unified-representative-claim");
  const fallbackPercentage = response.headers.get("anthropic-ratelimit-unified-fallback-percentage");
  const overageStatus = response.headers.get("anthropic-ratelimit-unified-overage-status");

  if (fiveHourUtilization !== null) {
    limits.unified_5h_utilization = parseFloat(fiveHourUtilization);
    limits.unified_5h_reset = parseInt(fiveHourReset);
    limits.unified_5h_status = fiveHourStatus;
  }
  if (sevenDayUtilization !== null) {
    limits.unified_7d_utilization = parseFloat(sevenDayUtilization);
    limits.unified_7d_reset = parseInt(sevenDayReset);
    limits.unified_7d_status = sevenDayStatus;
  }
  if (unifiedStatus) limits.unified_status = unifiedStatus;
  if (unifiedReset) limits.unified_reset = parseInt(unifiedReset);
  if (representativeClaim) limits.unified_representative_claim = representativeClaim;
  if (fallbackPercentage) limits.unified_fallback_percentage = parseFloat(fallbackPercentage);
  if (overageStatus) limits.unified_overage_status = overageStatus;

  return Object.keys(limits).length > 0 ? limits : null;
}

/**
 * Create a TransformStream that passes SSE data through unchanged
 * while extracting token counts from message_start and message_delta events.
 *
 * When the stream ends, calls onUsage({ input_tokens, output_tokens,
 * cache_read_tokens, cache_creation_tokens }).
 */
export function createUsageTrackingStream(onUsage) {
  let inputTokens = 0;
  let outputTokens = 0;
  let cacheReadTokens = 0;
  let cacheCreationTokens = 0;
  let buffer = "";

  function parseEvent(event) {
    if (event.type === "message_start" && event.message?.usage) {
      inputTokens = event.message.usage.input_tokens || 0;
      cacheReadTokens = event.message.usage.cache_read_input_tokens || 0;
      cacheCreationTokens = event.message.usage.cache_creation_input_tokens || 0;
    }
    if (event.type === "message_delta" && event.usage) {
      outputTokens = event.usage.output_tokens || 0;
    }
  }

  return new TransformStream({
    transform(chunk, controller) {
      controller.enqueue(chunk);

      const text = new TextDecoder().decode(chunk);
      buffer += text;

      const lines = buffer.split("\n");
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const data = line.slice(6).trim();
        if (data === "[DONE]") continue;

        try {
          parseEvent(JSON.parse(data));
        } catch {
          // Not valid JSON, skip
        }
      }
    },
    flush() {
      if (buffer.startsWith("data: ")) {
        const data = buffer.slice(6).trim();
        if (data !== "[DONE]") {
          try {
            parseEvent(JSON.parse(data));
          } catch {}
        }
      }

      onUsage({
        input_tokens: inputTokens,
        output_tokens: outputTokens,
        cache_read_tokens: cacheReadTokens,
        cache_creation_tokens: cacheCreationTokens,
      });
    },
  });
}
