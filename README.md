# llm-api-gateway

`llm-api-gateway` is a small Go gateway for OpenAI-compatible `/v1/responses` traffic. It load-balances first turns across downstream accounts, keeps carrier-bound continuations pinned to their owner, and archives hot routing metadata in SQLite plus content-level turn data in DuckDB.

Detailed design notes live in `/root/docs/personal-proj/llm-api-gateway/`.

## Routing Model

- `strict`: a request contains a known real `reasoning` or `compaction` carrier, and SQLite maps that carrier to one owner account. The request is forwarded to that owner with the original body.
- `replay`: a request has no carrier, or has unknown/conflicting carriers and replay is enabled. The gateway strips `reasoning` and `compaction` carrier items, preserves normal input items, chooses an account, and forwards the replay body.
- The gateway does not rewrite normal messages, `call_id`, `previous_response_id`, or add protocol headers downstream.
- Streaming responses are passed through as streaming responses. Once bytes are written to the client, the gateway does not retry that request.

## Accounts File

Set `LLM_GATEWAY_ACCOUNTS_FILE` to a JSON array:

```json
[
  {
    "account_id": "acc_40000",
    "provider_kind": "copilot-api",
    "display_name": "copilot 40000",
    "downstream_host": "127.0.0.1",
    "downstream_port": 40000,
    "enabled": true,
    "state": "running",
    "model_allowlist": [],
    "weight": 1
  }
]
```

## Configuration

Important environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `LLM_GATEWAY_LISTEN_ADDR` | `:8080` | HTTP listen address |
| `LLM_GATEWAY_SQLITE_PATH` | `var/llm-api-gateway.sqlite3` | SQLite hot metadata path |
| `LLM_GATEWAY_DUCKDB_PATH` | `var/llm-api-gateway.duckdb` | DuckDB archive path |
| `LLM_GATEWAY_ACCOUNTS_FILE` | empty | Downstream account registry |
| `LLM_GATEWAY_CARRIER_HMAC_KEY` | `dev-only-unsafe-key` | HMAC key for carrier indexes; set a real secret in deployment |
| `LLM_GATEWAY_ACCESS_TOKEN` | empty | Optional bearer/basic/cookie token for gateway routes |
| `LLM_GATEWAY_REPLAY_ENABLED` | `true` | Enable strip-carrier replay for unknown/conflict carriers |
| `LLM_GATEWAY_RETRY_MAX_ATTEMPTS` | `8` | Total upstream attempts per request |
| `LLM_GATEWAY_RETRY_BACKOFF` | `200ms` | Delay between retry attempts |
| `LLM_GATEWAY_STRICT_REPLAY_RETRY` | `false` | Allow strict-owner retry to strip carriers and replay on another account |
| `LLM_GATEWAY_ACTIVE_WINDOW` | `30m` | Active session window for least-active selection |
| `LLM_GATEWAY_SESSION_RETENTION` | `336h` | Retention for inactive owner mappings |
| `LLM_GATEWAY_UPSTREAM_TIMEOUT` | `5m` | HTTP client timeout to downstream |
| `LLM_GATEWAY_OTEL_STDOUT` | `false` | Print traces to stdout for local debugging |

Retryable upstream failures are network/request failures before response, HTTP `429`, `502`, `503`, `504`, and non-stream upstream bodies that are not valid JSON, including HTML error pages. Invalid client request bodies and deterministic upstream `4xx` responses other than `429` are not retried.

## Run

```bash
go run ./cmd/llm-api-gateway
```

Useful endpoints:

- `GET /healthz`
- `GET /readyz`
- `POST /v1/responses`
- `GET /admin`
- `GET /admin/api/accounts`
- `GET /admin/api/metrics`
- `GET /metrics`

If `LLM_GATEWAY_ACCESS_TOKEN` is set, pass `Authorization: Bearer <token>` or `X-LLM-Gateway-Token: <token>`.

## Test

```bash
go test ./...
```

Real downstream E2E is gated:

```bash
LLM_GATEWAY_REAL_E2E=1 \
LLM_GATEWAY_REAL_API_KEY=... \
LLM_GATEWAY_REAL_ACCOUNTS_JSON='[...]' \
go test ./internal/app -run TestRealDownstream
```

## Operations Notes

- Keep `LLM_GATEWAY_CARRIER_HMAC_KEY` stable for an existing SQLite database. Changing it makes old carrier indexes unusable.
- `LLM_GATEWAY_STRICT_REPLAY_RETRY=false` is the safer default. Turn it on only when automatic carrier-stripped recovery is preferable to failing a strict continuation.
- Admin account disable/enable/probe actions are available under `/admin`.
- Accounts that return retryable failures are put into short cooldown so later attempts can use healthier accounts.
