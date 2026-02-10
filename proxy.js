/**
 * ClawPool edge proxy — the actual deployed code that handles every
 * API request through proxy.clawpool.ai.
 *
 * This file is published to a public repo so you can see exactly what
 * happens to your requests. The imports reference private modules that
 * aren't included — you can read the full flow, but can't run it in
 * isolation.
 */

import { sessionId, resolveToken, resolveTokenPreferred, rotateToken } from "./auth.js";
import { totalTokens, updateUsage, deductCredits, updatePoolUsage, updateTokenRateLimits, checkUsageThresholds } from "./usage.js";
import { recordUsage } from "./ledger.js";
import { incrementHourlyCounter } from "./status.js";
import { attemptAutoRefresh } from "./stripe.js";

const UPSTREAM = "https://api.anthropic.com";

export async function handleProxyRequest(request, url, env, ctx) {

  const apiKey = extractPoolKey(request.headers.get("authorization"))
    || request.headers.get("x-api-key")
    || "";

  const poolUser = await env.POOL.get(`poolkey:${apiKey}`, "json");
  if (!poolUser) {
    ctx.waitUntil(incrementHourlyCounter(false, env));
    return Response.json({ error: "Invalid pool key" }, { status: 401 });
  }

  const { resolved, isOwnToken, isProviderPoolKey, error } = await resolveAuthorizedToken(apiKey, poolUser, env);
  if (error) {
    ctx.waitUntil(incrementHourlyCounter(false, env));
    return error;
  }

  const bodyBytes = request.body ? await request.arrayBuffer() : null;
  let response = await forwardToAnthropic(request, url, resolved.token.oauth_token, bodyBytes);

  if (response.status === 401 || response.status === 403 || response.status === 429) {
    const fallback = isOwnToken
      ? await resolveToken(await sessionId(apiKey), env)
      : await rotateToken(await sessionId(apiKey), resolved.tokenIndex, env);
    if (fallback) {
      response = await forwardToAnthropic(request, url, fallback.token.oauth_token, bodyBytes);
      return handleResponse(response, fallback.tokenIndex, apiKey, env, ctx, false, false);
    }
  }

  return handleResponse(response, resolved.tokenIndex, apiKey, env, ctx, isOwnToken, isProviderPoolKey);
}

function handleResponse(response, tokenIndex, apiKey, env, ctx, isOwnToken, isProviderPoolKey) {
  ctx.waitUntil(incrementHourlyCounter(response.ok, env));

  const rateLimits = extractRateLimits(response);
  if (rateLimits) {
    ctx.waitUntil(updateTokenRateLimits(tokenIndex, rateLimits, env));
  }

  const contentType = response.headers.get("content-type") || "";
  if (!response.ok || !contentType.includes("text/event-stream")) {
    return response;
  }

  let usageResolve;
  const usagePromise = new Promise((resolve) => { usageResolve = resolve; });
  const trackedBody = response.body.pipeThrough(createUsageTrackingStream((usage) => usageResolve(usage)));

  ctx.waitUntil(
    usagePromise.then(async (usage) => {
      await Promise.all([
        updateUsage(tokenIndex, usage, env),
        updateTokenRateLimits(tokenIndex, null, env, usage),
        ...(!isOwnToken ? [
          deductCredits(apiKey, usage, env),
          recordUsage(apiKey, tokenIndex, usage, env),
          updatePoolUsage(tokenIndex, usage, env),
        ] : isProviderPoolKey ? [
          deductCredits(apiKey, usage, env),
        ] : []),
      ]);
      if (!isOwnToken) {
        await checkUsageThresholds(apiKey, env).catch(() => {});
      }
    })
  );

  return new Response(trackedBody, { status: response.status, headers: response.headers });
}

async function resolveAuthorizedToken(apiKey, poolUser, env) {
  const isProviderPoolKey = poolUser.provider_key === true;
  const tokenLimit = poolUser.plan_tokens || 0;
  const tokensUsed = poolUser.tokens_used_period || 0;
  const subscriptionExhausted = tokenLimit > 0 && tokensUsed >= tokenLimit;

  if (subscriptionExhausted && !isProviderPoolKey) {
    if (poolUser.auto_refresh_enabled) {
      const topupResult = await attemptAutoRefresh(apiKey, poolUser, env);
      if (!topupResult.success) {
        return { error: Response.json({
          error: `Auto refresh failed: ${topupResult.error}. Update your payment method at https://clawpool.ai/dashboard`,
          tokens_used: tokensUsed, token_limit: tokenLimit,
        }, { status: 429 }) };
      }
    } else {
      return { error: Response.json({
        error: "You've used all your tokens this period. Enable auto refresh ($8 for 4M tokens) or wait for your next billing cycle. https://clawpool.ai/dashboard",
        tokens_used: tokensUsed, token_limit: tokenLimit,
      }, { status: 429 }) };
    }
  }

  let resolved = null;
  let isOwnToken = false;
  const ownerTokenIndices = poolUser.owner_token_indices || [];

  if (ownerTokenIndices.length > 0) {
    resolved = await resolveTokenPreferred(ownerTokenIndices, env);
    if (resolved) isOwnToken = true;
  }

  const canUsePool = !isProviderPoolKey || (tokenLimit > 0 && !subscriptionExhausted);
  if (!resolved && canUsePool) {
    resolved = await resolveToken(await sessionId(apiKey), env);
  }

  if (!resolved) {
    return { error: new Response(
      JSON.stringify({ type: "error", error: { type: "overloaded_error",
        message: isProviderPoolKey
          ? "Your token is at capacity or disabled. Wait for the rate limit to reset or add another token."
          : "No tokens available in the pool. All provider tokens are at capacity or disabled.",
      }}),
      { status: 529, headers: { "Content-Type": "application/json", "Retry-After": "300" } },
    ) };
  }

  return { resolved, isOwnToken, isProviderPoolKey };
}

function extractPoolKey(authHeader) {
  if (!authHeader) return null;
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1] : null;
}

function forwardToAnthropic(originalRequest, url, resolvedToken, bodyBytes) {
  const headers = new Headers(originalRequest.headers);
  headers.delete("x-api-key");
  headers.set("Authorization", `Bearer ${resolvedToken}`);
  headers.set("Host", "api.anthropic.com");

  return fetch(UPSTREAM + url.pathname + url.search, {
    method: originalRequest.method,
    headers,
    body: bodyBytes,
  });
}

function extractRateLimits(response) {
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

function createUsageTrackingStream(onUsage) {
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
