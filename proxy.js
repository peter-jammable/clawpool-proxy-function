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
import { totalTokens, checkUsageThresholds, updateNvidiaUsage } from "./usage.js";
import { recordUsage, recordNvidiaUsage } from "./ledger.js";
import { updateStatusAudit } from "./status.js";
import { sendThrottledCapacityAlert, sendProviderCapacityUpsell, sendAbuseAutoBlockAlert } from "./email.js";
import { attemptAutoRefresh, endTrialAndActivate } from "./stripe.js";
import { attemptPartnerAutoRefresh } from "./partner.js";
import { getPlans, getPriceForTeam, formatDollars } from "./pricing.js";
import { locationHintForRegion } from "./regions.js";
import { updateNvidiaTokenHealth } from "./nvidia-auth.js";
import { resolveAutoModel } from "./nvidia-benchmark.js";
import {
  translateAnthropicRequestToOpenai,
  translateOpenaiResponseToAnthropic,
  createOpenaiToAnthropicStream,
} from "./claude-to-openai.js";

const UPSTREAM = "https://api.anthropic.com";

/**
 * Get the ConsumerBilling DO stub for a pool user.
 */
function getConsumerBillingStub(env, poolUser) {
  const billingDoId = env.CONSUMER_BILLING.idFromName(`team:${poolUser.team_id}`);
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

    // Resolve billing record from team
    const billingRecord = await env.POOL.get(`team:${poolUser.team_id}`, "json");
    if (!billingRecord) return Response.json({ error: "Team not found" }, { status: 500 });

    // Team-level block check (admin can block entire accounts)
    if (billingRecord.blocked) {
      return Response.json({ error: "This account has been blocked. Contact support@clawpool.ai" }, { status: 403 });
    }

    // --- ConsumerBilling DO gate: authorize before forwarding ---
    const billingStub = getConsumerBillingStub(env, poolUser);
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
      if (error.status === 529) {
        ctx.waitUntil(updateStatusAudit(false, env));
        ctx.waitUntil(sendThrottledCapacityAlert("claude", env));
        // Provider with no consumer subscription — upsell the consumer tier
        if (poolUser.provider_key && !(billingRecord.plan_tokens > 0)) {
          ctx.waitUntil(sendProviderCapacityUpsell(poolUser.team_id, env));
        }
        // Attempt NVIDIA fallback when all Claude tokens are at capacity
        const fallbackBodyBytes = request.body ? await request.arrayBuffer() : null;
        const nvidiaFallbackResponse = await attemptNvidiaFallback(fallbackBodyBytes, apiKey, poolUser, billingStub, env, ctx);
        if (nvidiaFallbackResponse) return nvidiaFallbackResponse;
      }
      return error;
    }

    const bodyBytes = request.body ? await request.arrayBuffer() : null;
    const requestedModelFamily = extractModelFamily(bodyBytes);
    let response = await forwardToAnthropic(request, url, resolved.token.oauth_token, bodyBytes, env, resolved.token.region);

    if (response.status === 401 || response.status === 403 || response.status === 429) {
      // On auth/permission errors, read the body to log and check for abuse
      if (response.status === 401 || response.status === 403) {
        const errorBody = await response.text().catch(() => "");
        console.error(
          `[proxy] Anthropic ${response.status} — token #${resolved.tokenIndex}, consumer ${apiKey.slice(0, 12)}…, team ${poolUser.team_id}: ${errorBody.slice(0, 300)}`,
        );

        if (isTokenKillingError(errorBody)) {
          console.error(
            `[proxy] ABUSE DETECTED — auto-blocking team ${poolUser.team_id}, pausing token #${resolved.tokenIndex}`,
          );
          // Block + pause synchronously to close the window before returning
          await blockConsumerAndPauseToken(env, poolUser.team_id, resolved.tokenIndex, response.status);
          // Email alert is fire-and-forget
          ctx.waitUntil(sendAbuseAlert(env, {
            consumerPoolKey: apiKey,
            teamId: poolUser.team_id,
            tokenIndex: resolved.tokenIndex,
            anthropicStatus: response.status,
            anthropicError: errorBody.slice(0, 500),
          }));
          return Response.json(
            { error: "This account has been blocked. Contact support@clawpool.ai" },
            { status: 403 },
          );
        }

        // Non-abuse auth error — reconstruct the response for downstream handling
        response = new Response(errorBody, { status: response.status, headers: response.headers });
      }

      // Trial users: no pool rotation, but try NVIDIA fallback
      if (isTrialToken) {
        const nvidiaFallbackResponse = await attemptNvidiaFallback(bodyBytes, apiKey, poolUser, billingStub, env, ctx);
        if (nvidiaFallbackResponse) return nvidiaFallbackResponse;
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

    // If Anthropic response is still not OK after retries, attempt NVIDIA fallback
    if (!response.ok) {
      const nvidiaFallbackResponse = await attemptNvidiaFallback(bodyBytes, apiKey, poolUser, billingStub, env, ctx);
      if (nvidiaFallbackResponse) return nvidiaFallbackResponse;
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

// ---------------------------------------------------------------------------
// Abuse detection — auto-block consumers that kill provider tokens
// ---------------------------------------------------------------------------

const TOKEN_KILLING_PATTERNS = [
  /credential.{0,30}(revoked|suspended|disabled|terminated)/i,
  /account.{0,30}(disabled|suspended|banned|terminated|closed)/i,
  /violat/i,
  /\babuse\b/i,
  /terms of service/i,
  /access.{0,20}(revoked|terminated|suspended)/i,
];

function isTokenKillingError(errorBody) {
  return TOKEN_KILLING_PATTERNS.some((pattern) => pattern.test(errorBody));
}

/**
 * Synchronously block the consumer's team and pause the provider token.
 * Must complete before returning the 403 to close the race window.
 */
async function blockConsumerAndPauseToken(env, teamId, tokenIndex, anthropicStatus) {
  const [teamRecord, tokenRecord] = await Promise.all([
    env.POOL.get(`team:${teamId}`, "json"),
    env.POOL.get(`token:${tokenIndex}`, "json"),
  ]);

  const writes = [];

  if (teamRecord && !teamRecord.blocked) {
    teamRecord.blocked = true;
    teamRecord.blocked_at = new Date().toISOString();
    teamRecord.blocked_reason = `auto-blocked: triggered Anthropic ${anthropicStatus} on token #${tokenIndex}`;
    writes.push(env.POOL.put(`team:${teamId}`, JSON.stringify(teamRecord)));
  }

  if (tokenRecord && tokenRecord.enabled !== false) {
    tokenRecord.enabled = false;
    tokenRecord.paused_at = new Date().toISOString();
    tokenRecord.paused_reason = `auto-paused: Anthropic ${anthropicStatus} triggered by consumer`;
    writes.push(env.POOL.put(`token:${tokenIndex}`, JSON.stringify(tokenRecord)));
  }

  await Promise.all(writes);
}

/**
 * Fire-and-forget: look up consumer email and send admin alert.
 */
async function sendAbuseAlert(env, { consumerPoolKey, teamId, tokenIndex, anthropicStatus, anthropicError }) {
  let consumerEmail = null;
  const poolKeyRecord = await env.POOL.get(`poolkey:${consumerPoolKey}`, "json");
  if (poolKeyRecord?.google_id) {
    const googleUser = await env.POOL.get(`google_user:${poolKeyRecord.google_id}`, "json");
    consumerEmail = googleUser?.email;
  }

  await sendAbuseAutoBlockAlert(env, {
    consumerEmail,
    consumerPoolKey,
    teamId,
    tokenIndex,
    anthropicStatus,
    anthropicError,
  });
}

// ---------------------------------------------------------------------------

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
    // Partner auto-refresh: reset immediately, no Stripe involved
    if (billingRecord.partner) {
      const partnerRefresh = await attemptPartnerAutoRefresh(apiKey, billingRecord, env, poolUser.team_id);
      if (!partnerRefresh.success) {
        return { error: Response.json({
          error: "Partner key refresh cap reached. Contact your provider to reset.",
          tokens_used: tokensUsed, token_limit: tokenLimit,
        }, { status: 429 }) };
      }
      // Refresh succeeded — fall through to token resolution
    }
    // Trial exhaustion: return 429 and trigger async upgrade to paid plan
    else if (billingRecord.is_trial) {
      return {
        error: Response.json({
          error: "Your free trial is complete! Your subscription is now active. Please retry your request.",
          trial_exhausted: true,
          tokens_used: tokensUsed, token_limit: tokenLimit,
        }, { status: 429 }),
        endTrial: { teamId: poolUser.team_id, poolKey: apiKey },
      };
    }

    else if (billingRecord.auto_refresh_enabled) {
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
        error: `You've used all your tokens this period. Enable auto refresh (${formatDollars(getPriceForTeam(getPlans(env), billingRecord).refreshPriceCents)}) or wait for your next billing cycle. https://clawpool.ai/dashboard`,
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

// ---------------------------------------------------------------------------
// NVIDIA fallback — transparent fallback to NVIDIA when Anthropic is unavailable
// ---------------------------------------------------------------------------

const NVIDIA_UPSTREAM = "https://integrate.api.nvidia.com";
const MAX_NVIDIA_FALLBACK_RETRIES = 2;

/**
 * Attempt to fulfill an Anthropic-format request via NVIDIA when Claude is unavailable.
 * Translates Anthropic → OpenAI, sends to NVIDIA, translates response back → Anthropic.
 * Returns null if fallback is not possible (no NVIDIA tokens, NVIDIA also fails, etc.).
 */
async function attemptNvidiaFallback(bodyBytes, apiKey, poolUser, billingStub, env, ctx) {
  if (!bodyBytes) return null;

  let anthropicBody;
  try {
    anthropicBody = JSON.parse(new TextDecoder().decode(bodyBytes));
  } catch {
    return null;
  }

  // Only fall back for actual message requests (not count_tokens, etc.)
  if (!anthropicBody.messages) return null;

  const originalModel = anthropicBody.model || "claude-sonnet-4-5-20250929";
  const resolvedModel = await resolveAutoModel(env);
  const openaiBody = translateAnthropicRequestToOpenai(anthropicBody, resolvedModel);

  const hash = await sessionId(apiKey);
  const resolved = await resolveToken(hash, env, "nvidia");
  if (!resolved) return null;

  // Forward to NVIDIA with retry logic (same pattern as nvidia-proxy.js)
  let response = await forwardToNvidiaFallback(resolved.token.api_key, openaiBody);
  let currentTokenIndex = resolved.tokenIndex;
  let retries = 0;

  while (!response.ok && retries < MAX_NVIDIA_FALLBACK_RETRIES) {
    const status = response.status;

    ctx.waitUntil(updateNvidiaTokenHealth(currentTokenIndex, false, status, env));

    if (status === 429) {
      const rotated = await rotateToken(hash, currentTokenIndex, env, "nvidia");
      if (!rotated) break;
      currentTokenIndex = rotated.tokenIndex;
      response = await forwardToNvidiaFallback(rotated.token.api_key, openaiBody);
    } else if (status >= 500) {
      if (retries === 0) {
        response = await forwardToNvidiaFallback(resolved.token.api_key, openaiBody);
      } else {
        const rotated = await rotateToken(hash, currentTokenIndex, env, "nvidia");
        if (!rotated) break;
        currentTokenIndex = rotated.tokenIndex;
        response = await forwardToNvidiaFallback(rotated.token.api_key, openaiBody);
      }
    } else {
      break;
    }

    retries++;
  }

  ctx.waitUntil(updateNvidiaTokenHealth(currentTokenIndex, response.ok, response.status, env));

  if (!response.ok) return null;

  console.log(`[proxy] NVIDIA fallback activated — model=${resolvedModel} nvidia_token=${currentTokenIndex} retries=${retries}`);

  const contentType = response.headers.get("content-type") || "";

  if (contentType.includes("text/event-stream")) {
    // Streaming: pipe through OpenAI→Anthropic translation stream
    let usageResolve;
    const usagePromise = new Promise((resolve) => { usageResolve = resolve; });
    const translatedBody = response.body.pipeThrough(
      createOpenaiToAnthropicStream(originalModel, (usage) => usageResolve(usage)),
    );

    ctx.waitUntil(
      usagePromise.then(async (usage) => {
        if (usage && (usage.input_tokens > 0 || usage.output_tokens > 0)) {
          const normalizedUsage = {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
          };
          const billableTokens = totalTokens(normalizedUsage);
          const kvTeamKey = poolUser.team_id ? `team:${poolUser.team_id}` : null;
          const kvSeatKey = `poolkey:${apiKey}`;
          const [deductResult] = await Promise.all([
            billingStub.fetch(new Request("https://do/deduct", {
              method: "POST",
              body: JSON.stringify({ billableTokens, seatKey: apiKey, kvTeamKey, kvSeatKey, teamId: poolUser.team_id }),
            })).then((deductResponse) => deductResponse.json()),
            updateNvidiaUsage(currentTokenIndex, normalizedUsage, env),
            recordNvidiaUsage(apiKey, currentTokenIndex, normalizedUsage, env),
          ]);
          if (deductResult.alertThreshold) {
            await checkUsageThresholds(apiKey, env).catch(() => {});
          }
        }
      }),
    );

    return new Response(translatedBody, {
      status: 200,
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
      },
    });
  } else {
    // Non-streaming: translate OpenAI JSON → Anthropic JSON
    const openaiResponse = await response.json();
    const anthropicResponse = translateOpenaiResponseToAnthropic(openaiResponse, originalModel);

    ctx.waitUntil((async () => {
      if (openaiResponse.usage) {
        const normalizedUsage = {
          input_tokens: openaiResponse.usage.prompt_tokens || 0,
          output_tokens: openaiResponse.usage.completion_tokens || 0,
          cache_read_tokens: 0,
          cache_creation_tokens: 0,
        };
        const billableTokens = totalTokens(normalizedUsage);
        const kvTeamKey = poolUser.team_id ? `team:${poolUser.team_id}` : null;
        const kvSeatKey = `poolkey:${apiKey}`;
        const [deductResult] = await Promise.all([
          billingStub.fetch(new Request("https://do/deduct", {
            method: "POST",
            body: JSON.stringify({ billableTokens, seatKey: apiKey, kvTeamKey, kvSeatKey, teamId: poolUser.team_id }),
          })).then((deductResponse) => deductResponse.json()),
          updateNvidiaUsage(currentTokenIndex, normalizedUsage, env),
          recordNvidiaUsage(apiKey, currentTokenIndex, normalizedUsage, env),
        ]);
        if (deductResult.alertThreshold) {
          await checkUsageThresholds(apiKey, env).catch(() => {});
        }
      }
    })());

    return Response.json(anthropicResponse);
  }
}

/**
 * Forward a request to NVIDIA upstream for fallback.
 * Includes a 60-second timeout to prevent hanging.
 */
function forwardToNvidiaFallback(nvidiaApiKey, body) {
  const abortController = new AbortController();
  const timeoutId = setTimeout(() => abortController.abort(), 60_000);

  return fetch(`${NVIDIA_UPSTREAM}/v1/chat/completions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${nvidiaApiKey}`,
    },
    body: JSON.stringify(body),
    signal: abortController.signal,
  }).finally(() => clearTimeout(timeoutId));
}

// ---------------------------------------------------------------------------

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
