# Header contract

Every inference request that passes through the llm-d Inference Gateway carries these headers. They are the
only way observe-d can tie the gateway, the EPP routing decision, the vLLM engine and the cost back to one
request and one tenant — agree them with the gateway owner before anything else (MVP.md §4).

| Header | Required | Set by | Notes |
|---|---|---|---|
| `traceparent` | yes | client, else the gateway creates it | W3C Trace Context. Must be forwarded to EPP and vLLM unchanged. Join key for `llm_requests`. |
| `x-request-id` | yes | client, else Envoy generates it | Human-friendly lookup key; fallback join key if vLLM does not propagate `traceparent`. |
| `x-org-id` | yes | **gateway, derived from the API key** | Never trusted from the client: the gateway overwrites any client value. |
| `x-app-id` | yes | **gateway, derived from the API key** | Same rule as `x-org-id`. |
| `x-user-id` | optional | client | Pseudonymized (hash / opaque ID). Never an email or name. |
| `x-session-id` | optional | client | Conversation / multi-turn grouping. |

## Where each header ends up

| Header | Gateway access log | EPP span | vLLM span | `llm_requests` column |
|---|---|---|---|---|
| `traceparent` | `TraceId`, `SpanId` | parent context | parent context | `trace_id` |
| `x-request-id` | `request_id` attribute | attribute | `gen_ai.request.id` (verify) | `request_id` |
| `x-org-id` | attribute | — | — | `org_id` |
| `x-app-id` | attribute | — | — | `app_id` |
| `x-user-id` | attribute | — | — | `user_id` |
| `x-session-id` | attribute | — | — | `session_id` |

## Rules

- High-cardinality IDs (`x-request-id`, `x-user-id`, `x-session-id`, trace IDs) are **never** used as metric
  labels — only on logs, spans and `llm_requests` rows. The collector drops them from metrics.
- Missing `x-org-id` / `x-app-id` is recorded as `unknown`, never dropped, and counted on the traceability
  dashboard so broken gateway config is visible.

## Open item (M1 spike)

Confirm on the pinned vLLM version that the engine extracts `traceparent` from the incoming request and which
span attributes it emits (request id, token counts, queue / prefill / decode / TTFT timings). Record the result
in `docs/spike-vllm-tracing.md`. If propagation does not work, the join key becomes `x-request-id`.
