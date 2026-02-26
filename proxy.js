/**
 * ClawPool edge proxy — the actual deployed code that handles every
 * API request through proxy.clawpool.ai.
 *
 * This file is published to a public repo so you can see exactly what
 * happens to your requests. The imports reference private modules that
 * aren't included — you can read the full flow, but can't run it in
 * isolation.
 */

import { sessionId, resolveToken, rotateToken } from "./token-routing.js";
import { resolveTokenPreferred } from "./auth.js";
import { totalTokens, checkUsageThresholds } from "./usage.js";
import { recordUsage } from "./ledger.js";
import { updateStatusAudit } from "./status.js";
import { attemptAutoRefresh, endTrialAndActivate } from "./stripe.js";
import { locationHintForRegion } from "./regions.js";

const UPSTREAM = "https://api.anthropic.com";

/**
 * Get the ConsumerBilling DO stub for a pool user.
 * Keyed by team ID (for team users) or pool key (for legacy solo users).
 */
function getConsumerBillingStub(env, poolUser, apiKey) {
  const billingDoId = poolUser.team_id
    ? env.CONSUMER_BILLING.idFromName(`team:${poolUser.team_id}`)
    : env.CONSUMER_BILLING.idFromName(`key:${apiKey}`);
  return env.CONSUMER_BILLING.get(billingDoId);
}

/**
 * Get the ProviderToken DO stub for a token index.
 */
function getProviderTokenStub(env, tokenIndex) {
  const doId = env.PROVIDER_TOKEN.idFromName(String(tokenIndex));
  return env.PROVIDER_TOKEN.get(doId);
}

export async function handleProxyRequest(request, url, env, ctx) {
  try {
    const apiKey = extractPoolKey(request.headers.get("authorization"))
      || request.headers.get("x-api-key")
      || "";

    const poolUser = await env.POOL.get(`poolkey:${apiKey}`, "json");
    if (!poolUser) {
      return Response.json({ error: "Invalid pool key" }, { status: 401 });
    }

    // Check seat is enabled (team seats can be disabled by admin)
    if (poolUser.enabled === false) {
      return Response.json({ error: "This pool key has been disabled by your team admin." }, { status: 403 });
    }

    // Resolve billing record: team record if team_id present, else poolkey itself (legacy)
    let billingRecord;
    if (poolUser.team_id) {
      billingRecord = await env.POOL.get(`team:${poolUser.team_id}`, "json");
      if (!billingRecord) return Response.json({ error: "Team not found" }, { status: 500 });
    } else {
      billingRecord = poolUser;
    }

    // --- ConsumerBilling DO gate: authorize before forwarding ---
    const billingStub = getConsumerBillingStub(env, poolUser, apiKey);
    const isProviderPoolKey = poolUser.provider_key === true;
    let doExhausted = false;

    if (!isProviderPoolKey && poolUser.trial_token_index === undefined) {
      const authResult = await billingStub
        .fetch(new Request("https://do/authorize", {
          method: "POST",
          body: JSON.stringify({ planTokens: billingRecord.plan_tokens || 0 }),
        }))
        .then((response) => response.json());

      if (!authResult.authorized) {
        // reason === "exhausted" — pass to resolveAuthorizedToken
        // which handles auto-refresh, trial exhaustion, etc.
        doExhausted = true;
      }
    }

    const { resolved, isOwnToken, isProviderPoolKey: isProviderKey, isTrialToken, error, endTrial } = await resolveAuthorizedToken(apiKey, poolUser, billingRecord, env, doExhausted);
    if (error) {
      if (endTrial) {
        ctx.waitUntil(endTrialAndActivate(endTrial.teamId, endTrial.poolKey, env));
      }
      if (error.status === 529) ctx.waitUntil(updateStatusAudit(false, env));
      return error;
    }

    const bodyBytes = request.body ? await request.arrayBuffer() : null;
    const requestedModelFamily = extractModelFamily(bodyBytes);
    let response = await forwardToAnthropic(request, url, resolved.token.oauth_token, bodyBytes, env, resolved.token.region);

    if (response.status === 401 || response.status === 403 || response.status === 429) {
      // Trial users: no pool fallback, return the error directly
      if (isTrialToken) {
        return response;
      }
      const fallback = isOwnToken
        ? await resolveToken(await sessionId(apiKey), env)
        : await rotateToken(await sessionId(apiKey), resolved.tokenIndex, env);
      if (fallback) {
        response = await forwardToAnthropic(request, url, fallback.token.oauth_token, bodyBytes, env, fallback.token.region);
        return handleResponse(response, fallback.tokenIndex, apiKey, poolUser, env, ctx, false, false, billingStub, requestedModelFamily);
      }
    }

    return handleResponse(response, resolved.tokenIndex, apiKey, poolUser, env, ctx, isOwnToken, isProviderKey, billingStub, requestedModelFamily);
  } catch (proxyError) {
    console.error("[handleProxyRequest] Unhandled error:", proxyError?.message || proxyError, proxyError?.stack);
    return Response.json(
      { error: `Proxy error: ${proxyError?.message || "unknown"}` },
      { status: 500 },
    );
  }
}

function handleResponse(response, tokenIndex, apiKey, poolUser, env, ctx, isOwnToken, isProviderPoolKey, billingStub, requestedModelFamily) {
  ctx.waitUntil(updateStatusAudit(response.ok, env));

  const rateLimits = extractRateLimits(response);
  const providerTokenStub = getProviderTokenStub(env, tokenIndex);

  // Route rate limit header updates through ProviderToken DO
  if (rateLimits) {
    ctx.waitUntil(
      providerTokenStub.fetch(new Request("https://do/update", {
        method: "POST",
        body: JSON.stringify({ tokenIndex, rateLimits, usage: null }),
      })),
    );
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
      const billableTokens = totalTokens(usage);

      // --- ProviderToken DO: rate limits + capacity scoring + daily usage ---
      await providerTokenStub.fetch(new Request("https://do/update", {
        method: "POST",
        body: JSON.stringify({ tokenIndex, rateLimits: null, usage }),
      }));

      await providerTokenStub.fetch(new Request("https://do/record-daily-usage", {
        method: "POST",
        body: JSON.stringify({ tokenIndex, billableTokens }),
      }));

      // --- Pool request tracking (non-own-token requests) ---
      if (!isOwnToken) {
        // ConsumerBilling DO: deduct credits
        const kvTeamKey = poolUser.team_id ? `team:${poolUser.team_id}` : null;
        const kvSeatKey = `poolkey:${apiKey}`;
        const deductResult = await billingStub
          .fetch(new Request("https://do/deduct", {
            method: "POST",
            body: JSON.stringify({ billableTokens, seatKey: apiKey, kvTeamKey, kvSeatKey, teamId: poolUser.team_id }),
          }))
          .then((response) => response.json());

        if (deductResult.alertThreshold) {
          await checkUsageThresholds(apiKey, env).catch(() => {});
        }

        // Ledger entry (already idempotent via unique IDs)
        await recordUsage(apiKey, tokenIndex, usage, env, rateLimits, requestedModelFamily);

        // ProviderToken DO: pool usage windows
        await providerTokenStub.fetch(new Request("https://do/record-pool-usage", {
          method: "POST",
          body: JSON.stringify({ tokenIndex, billableTokens }),
        }));
      } else if (isProviderPoolKey) {
        // Provider pool key: deduct credits from their consumer allocation
        const kvTeamKey = poolUser.team_id ? `team:${poolUser.team_id}` : null;
        const kvSeatKey = `poolkey:${apiKey}`;
        await billingStub.fetch(new Request("https://do/deduct", {
          method: "POST",
          body: JSON.stringify({ billableTokens, seatKey: apiKey, kvTeamKey, kvSeatKey, teamId: poolUser.team_id }),
        }));
      }

      // Update session tokens_served for usage-based rotation
      const sessionHash = await sessionId(apiKey);
      const sessionKey = `session:${sessionHash}`;
      const session = await env.POOL.get(sessionKey, "json");
      if (session) {
        session.tokens_served = (session.tokens_served || 0) + billableTokens;
        const ttl = parseInt(env.SESSION_TTL || "3600", 10);
        await env.POOL.put(sessionKey, JSON.stringify(session), { expirationTtl: ttl });
      }
    }),
  );

  return new Response(trackedBody, { status: response.status, headers: response.headers });
}

async function resolveAuthorizedToken(apiKey, poolUser, billingRecord, env, doExhausted = false) {
  const isProviderPoolKey = poolUser.provider_key === true;
  const tokenLimit = billingRecord.plan_tokens || 0;
  const tokensUsed = billingRecord.tokens_used_period || 0;
  // DO is the authoritative source for exhaustion — KV is a fallback for
  // provider pool keys and trial users that skip the DO gate
  const subscriptionExhausted = doExhausted || (tokensUsed >= tokenLimit);

  if (subscriptionExhausted && !isProviderPoolKey) {
    // Trial exhaustion: return 429 and trigger async upgrade to paid plan
    if (billingRecord.is_trial) {
      return {
        error: Response.json({
          error: "Your free trial is complete! Your subscription is now active. Please retry your request.",
          trial_exhausted: true,
          tokens_used: tokensUsed, token_limit: tokenLimit,
        }, { status: 429 }),
        endTrial: { teamId: poolUser.team_id, poolKey: apiKey },
      };
    }

    if (billingRecord.auto_refresh_enabled) {
      const teamId = poolUser.team_id || null;
      const refreshResult = await attemptAutoRefresh(apiKey, billingRecord, env, teamId);
      if (!refreshResult.success) {
        const manageUrl = refreshResult.portal_url || "https://clawpool.ai/dashboard";
        return { error: Response.json({
          error: `Auto refresh failed. Update your payment method: ${manageUrl}`,
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

  // Trial token routing: pick the best available admin token
  if (poolUser.trial_token_index !== undefined) {
    const { decryptToken } = await import("./crypto.js");
    const { resolveAdminTokens } = await import("./dashboard.js");
    const { isProviderActive } = await import("./token-routing.js");
    const adminTokens = await resolveAdminTokens(env);
    const now = Date.now();

    // Pick first admin token that's currently active (or has no active hours restriction)
    let chosen = null;
    for (const entry of adminTokens) {
      if (!entry.token.timezone || entry.token.timezone === "any" || !entry.token.active_hours) {
        chosen = entry;
        break;
      }
      if (isProviderActive(entry.token, now)) {
        chosen = entry;
        break;
      }
    }

    if (chosen) {
      const { tokenIndex, token } = chosen;
      if (token.encrypted && env.TOKEN_ENCRYPTION_KEY) {
        token.oauth_token = await decryptToken(token.oauth_token, env.TOKEN_ENCRYPTION_KEY);
      }
      return { resolved: { tokenIndex, token }, isOwnToken: false, isProviderPoolKey: false, isTrialToken: true };
    }
    return { error: Response.json({ error: "Trial service temporarily unavailable." }, { status: 503 }) };
  }

  let resolved = null;
  let isOwnToken = false;
  const ownerTokenIndices = poolUser.owner_token_indices || [];

  if (ownerTokenIndices.length > 0) {
    resolved = await resolveTokenPreferred(ownerTokenIndices, env);
    if (resolved) isOwnToken = true;
  }

  const canUsePool = !isProviderPoolKey || (billingRecord.plan_tokens > 0 && !subscriptionExhausted);
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

  return { resolved, isOwnToken, isProviderPoolKey, isTrialToken: false };
}

/**
 * Extract the model family (opus, sonnet, haiku) from raw request body bytes.
 * Regex on the raw string — no JSON parsing needed.
 */
function extractModelFamily(bodyBytes) {
  if (!bodyBytes) return null;
  try {
    const text = new TextDecoder().decode(bodyBytes.slice(0, 500)); // only need the start
    const match = text.match(/\b(opus|sonnet|haiku)\b/i);
    return match ? match[1].toLowerCase() : null;
  } catch {
    return null;
  }
}

function extractPoolKey(authHeader) {
  if (!authHeader) return null;
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1] : null;
}

function forwardToAnthropic(originalRequest, url, resolvedToken, bodyBytes, env, tokenRegion) {
  const headers = new Headers(originalRequest.headers);
  headers.delete("x-api-key");
  headers.set("Authorization", `Bearer ${resolvedToken}`);
  headers.set("Host", "api.anthropic.com");

  const beta = headers.get("anthropic-beta") || "";
  if (!beta.includes("oauth-2025-04-20")) {
    headers.set("anthropic-beta", beta ? `${beta},oauth-2025-04-20` : "oauth-2025-04-20");
  }

  const upstreamUrl = UPSTREAM + url.pathname + url.search;
  const upstreamRequest = new Request(upstreamUrl, {
    method: originalRequest.method,
    headers,
    body: bodyBytes,
  });

  // Route through regional DO if the token has a region
  if (tokenRegion && env.REGIONAL_PROXY) {
    const locationHint = locationHintForRegion(tokenRegion);
    const durableObjectId = env.REGIONAL_PROXY.idFromName(tokenRegion);
    const stub = env.REGIONAL_PROXY.get(durableObjectId, { locationHint });
    return stub.fetch(upstreamRequest);
  }

  return fetch(upstreamRequest);
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
