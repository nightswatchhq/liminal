# RFC-LIM-001 — Customs: a compliance-gated transfer indexer, and the Liminal work to enable it

- **Status:** Draft
- **Author:** Pete
- **Target:** `examples/customs/` in `nuthatch-org/liminal`
- **Base:** Wasmtime 44 / WASIp2 (WASI 0.2.x) — the runtime the repo already targets
- **Related:** GRC-007 ("Conduit"), [`research.md`](../../research.md), [`examples/uni-v3-swaps`](../uni-v3-swaps)

---

## Summary

Customs is the second example pipeline for Liminal and its first *compliance-grade* one. It indexes ERC-20 transfers for a basket of tokens on Ethereum mainnet, screens every counterparty against a sanctions list **before any event reaches the system-of-record**, enriches cleared transfers with USD price at block time, and fans the result out to Postgres and Kafka from a single cursor. Flagged events are routed to a quarantine store and never touch the writer; events the screener cannot resolve are held (fail-closed) rather than written.

It exists to demonstrate the one thing no Subgraph + Substreams + sidecar stack can express: **architectural non-ingestion under capability isolation** — the writer component is *structurally* incapable of receiving a flagged event or of making a network call, and that fact is a verifiable property of a signed composition rather than a code-review promise.

This RFC specifies both the application and the delta on the Liminal host/SDK/WIT required to run it.

---

## 1. Motivation

`research.md` §3.8 makes the case in the abstract; Customs makes it concrete and runnable. The decision logic for a real adopter:

A regulated entity (licensed exchange, stablecoin issuer, bank digital-asset desk) has a hard control under OFAC/MiCA/BSA: **activity from sanctioned addresses must not be ingested into the system of record** — not filtered on read, not present-then-redacted. Their auditor must attest the control is architectural.

- **Substreams can't** make the screening call at all — pure-compute by design. The effectful step must leave the module.
- **Substreams + custom effectful sink** *works mechanically* (the sink calls the screening API and drops before `INSERT`), but the control now lives in the sink's `if` statement, the flagged data has already transited the gRPC stream and the sink's process memory before the drop, and the attestation is "we reviewed the sink code." Two cursors, a reconciliation layer.
- **Customs on Liminal** makes the screen a capability-isolated component with `wasi:http` granted only to the screening origin, and a writer component that imports **no** HTTP and **never receives** a flagged event because the drop happens upstream in the DAG. The attestation becomes: *the writer is structurally incapable of seeing flagged events, and here is the signed composition proving the gate sits before it.*

The load-bearing reason is exactly one capability — a compliance guarantee **enforced by capability isolation** — which neither incumbent can express (Substreams has no effects; Subgraph AS mappings and monolithic sinks have no per-component capability boundary). Enrichment and multi-sink ride along for free once the runtime is adopted for the gate.

### 1.1 Coverage of the `research.md` capability gaps

Customs deliberately exercises the gaps that are shippable on WASIp2 today and deliberately defers the two that depend on unstabilized streaming:

| `research.md` gap | Exercised? | How |
|---|---|---|
| §3.1 off-chain HTTP enrichment | **Yes** | `price-enricher` → oracle origin |
| §3.2 isolation of untrusted middleware | **Partial** | per-component caps; screener/enricher each scoped to one origin |
| §3.3 black-box cache the mapping can't see | **Yes** | namespaced `wasi:keyvalue` (`verdicts`, `prices`) |
| §3.4 mid-block partial emission | **No (deliberate)** | batch pipeline; deferred to a future streaming example |
| §3.5 polyglot stages | **No (deliberate)** | all-Rust; polyglot deferred |
| §3.6 hot-swap without source resync | **Optional** | swap `screener.wasm` (same WIT); source cursor preserved |
| §3.7 multiple sinks off one cursor | **Yes** | fan-out to Postgres + Kafka |
| §3.8 effectful precondition / drop-before-write | **Yes — the centerpiece** | verdict routing; writer never sees flagged |

### 1.2 Why this showcase does not use WASIp3 streaming

The governed-effectfulness differentiator is fully demonstrable on the WASIp2 / Wasmtime 44 base the repo already targets. Mid-block `stream<T>` emission (§3.4) is the most distinctive *latency* feature, but it is also the one gated on an unstabilized spec (WASI 0.3, completion tracked ~early 2026). Customs is the *governance* showcase, not the *latency* showcase, so it is built entirely on stable ground and does not gate the compliance story on streaming. A separate `examples/streaming-orderbook/` can carry the WASIp3 story once it lands. This is the same discipline argued elsewhere: lead with what ships, never with the promise.

### 1.3 Relationship to the existing PoC

Customs is the `uni-v3-swaps` PoC's compliance-grade sibling: same host, same WASIp2 base, same multi-sink fan-out — **plus** the three capabilities the PoC does not exercise: origin-scoped HTTP, conditional routing, and namespaced key-value. The platform work below is largely the work of turning those three from "described in `research.md`" into "enforced by the host."

---

## 2. Scope

**In scope**
- The Customs application: components, WIT, data model, routing, fail-closed semantics.
- The Liminal host/SDK/WIT delta required to run it (Section 5 onward).
- A declarative, content-addressed, signable pipeline manifest (Section 6).
- Host-enforced capability scoping: HTTP origin allow-lists and key-value namespacing (Section 7).
- An offline demo harness (local screening server + fixtures + infra) so the example runs without a paid screening provider (Section 8).

**Out of scope**
- WASIp3 streaming / mid-block emission (deferred; §1.2).
- Polyglot components (deferred; all-Rust here).
- Production hardening: HA, signing-key custody, exactly-once delivery, screening-provider SLAs.
- Network indexing / PoI. **Customs is non-deterministic by construction** (it makes HTTP calls), therefore it is an operator-run, off-network pipeline and is **not eligible for indexing rewards or PoI.** This is stated plainly so no reader mistakes it for a network-served subgraph.

---

## 3. The application

### 3.1 What it indexes

ERC-20 `Transfer(address indexed from, address indexed to, uint256 value)` events for a configurable basket — USDC, WETH, WBTC by default. USDC gives a high-volume screening workload; WETH/WBTC give real price volatility so the enrichment step is non-trivial.

### 3.2 Pipeline DAG

```
source (EVM logs: Transfer topic, basket addresses)
  └─► decoder            [no capabilities]
        └─► screener     [http: screening-origin only | kv: "verdicts"]
              │  emits verdict = cleared | flagged | indeterminate
              ├─(cleared)──────► price-enricher  [http: oracle-origin only | kv: "prices"]
              │                       └─► (fan-out)
              │                             ├─► sink-postgres   [postgres only — NO http]
              │                             └─► sink-kafka      [kafka only]
              ├─(flagged)──────► sink-quarantine [quarantine store only]
              └─(indeterminate)► sink-hold       [kv: "hold" — durable; re-screened]
```

The shape is the entire argument:

- `decoder` and `sink-postgres` import **no** `wasi:http`. They cannot call out. This is not policy — it is the Component Model: an interface a component's `world` does not import does not exist for that component.
- `screener` and `price-enricher` each import `wasi:http`, but the host restricts each to **one** origin (Section 7). Least privilege per stage, not "the sink can call anything."
- The screener's output is a tagged union; the **host** routes on the discriminant. The Postgres writer is reachable only via the `cleared → enricher → postgres` path, so a `flagged` or `indeterminate` event has no edge that reaches it.

### 3.3 Data model (WIT)

See the RFC body in version control history for the full WIT sketch. In the implementation the inter-component wire types are serde structs (the runtime moves opaque JSON bytes along edges; see the repo README "How it works"). The compliance-critical part is the **worlds**: each component declares exactly its capabilities, and the writer's omission of `http` is the artifact an auditor verifies.

### 3.4 Fail-closed semantics

A compliance gate that fails *open* (writes unscreened data when the provider is down) defeats its own purpose. Customs is fail-closed:

- Screening provider reachable, address on list → `flagged` → `sink-quarantine`.
- Reachable, address not on list → `cleared` → `enricher` → SoR.
- **Unreachable, timeout, or non-2xx → `indeterminate` → `sink-hold`.** The event is captured in a **durable** hold store; a background re-screen loop retries with backoff and, on a later `cleared`/`flagged`, the event is re-injected at the gate. Because the hold store is durable, the **source cursor may advance** — the event is not lost on restart.

Default behavior is **hold + retry**, configurable to **halt** (cursor stops advancing until the provider recovers).

### 3.5 Cache semantics and invalidation

- `screener` memoizes verdicts in the `verdicts` key-value namespace keyed by `(counterparty, list-version)`. A cached `cleared` keyed by an *old* `list-version` is never served once the list updates, because the key changes. A short TTL (default 24h) bounds staleness within a list version.
- `price-enricher` memoizes prices in `prices` keyed by `(token, block-number)` — immutable, so no invalidation needed.
- Neither cache is readable by any other component; the host scopes keys by namespace (Section 7).

### 3.6 Multi-sink commit semantics

Customs treats the **Postgres write as the commit point**; the Kafka publish is **at-least-once after commit**. A Kafka outage never blocks or loses the SoR write; a record may be published more than once on retry — downstream consumers must be idempotent on `(tx-hash, log-index)`. Exactly-once across the fan-out is out of scope.

---

## 4. Why each incumbent can't do *this* pipeline

- **Subgraph:** no way to make the screening or price HTTP call from a mapping. "Index everything, filter on read" means the sanctioned transfer is in the store.
- **Substreams:** pure-compute; cannot screen or price in-module. A custom effectful sink can mechanically drop before write, but the control is sink code and the flagged data has already transited the stream.
- **Customs:** the gate is a separate, capability-isolated component upstream of a writer that has no edge from the `flagged` branch and no HTTP grant. The control is the topology, and the topology is signed.

---

## 5. Liminal platform work (the delta)

### W1 — Declarative, content-addressed manifest *(host)*
TOML manifest describing components (with `sha256` content addresses), per-component capability grants, and edges. Secrets/endpoints injected via `${VAR}` and excluded from the signed body. Deliver: manifest schema + loader; `liminal compose hash`.

### W2 — HTTP origin allow-list enforcement *(host + sdk)*
The host's `wasi:http/outgoing-handler` checks each request's scheme+authority against the component's `allow_origins`. Deliver: policy-wrapping outgoing handler; SDK error type.

### W3 — Conditional / multi-output routing *(host + wit)*
A component emits a tagged value; the host dispatches to the downstream edge matching the active case (`when = "cleared" | …`). Unconditional fan-out remains supported. Deliver: discriminant-aware dispatch; manifest `when` on edges.

### W4 — `wasi:keyvalue` provider with namespace scoping *(host + sdk)*
Host-provided `wasi:keyvalue/store`, in-memory (default) or Redis, transparently prefixing keys by the component's `namespace`. TTL for `verdicts`. Deliver: provider + per-component namespace binding; SDK helper.

### W5 — EVM log source generalization *(host/sdk)*
Configurable log filter: `topic0` + address set, over WebSocket RPC. Plus a `fixture` source for offline runs. Deliver: generalized `evm-logs` source + fixture source.

### W6 — Application components *(`examples/customs/`)*
`decoder`, `screener`, `price-enricher`, `sink-postgres`, `sink-kafka`, `sink-quarantine`, `sink-hold`.

### W7 — Offline demo harness *(tooling)*
Local `screening-server`, fixtures, `docker-compose` (Postgres/Kafka/Redis/screening-server), `run.sh`.

### W8 — Compose signing/verification + audit-artifact doc *(host + docs)*
`liminal compose sign` / `verify` over the canonical hash from W1 (ed25519/minisign for the example; sigstore/cosign noted as production). Audit-artifact doc.

**Dependency order:** W1 → W2/W3/W4/W5 (parallel) → W6 → W7; W8 parallel after W1.

---

## 6. Manifest design

See `examples/customs/customs.pipeline.toml`. The signature covers the canonical serialization of topology (component ids + `sha256` + capability *declarations*) and the edge set; it excludes `${...}` runtime values. The same signed composition runs in staging and prod with different endpoints.

---

## 7. Capability enforcement (the security core)

**Layer 1 — grant (Component-Model native, the strong layer).** A component can invoke only the interfaces its `world` imports. The writer does not import `wasi:http`, so it **cannot** make an HTTP call — enforced by the component linker. (Liminal proves this with a test: a component that imports `wasi:http` fails to instantiate without the `http` grant.)

**Layer 2 — policy (host-implemented, least privilege).** For components that *do* import a capability: HTTP origin allow-lists and key-value namespace scoping.

**The auditor's check reduces to two facts in the signed manifest:**
1. `sink-postgres` has no `http` capability (cannot call out).
2. The only edge into `sink-postgres` is `enricher`, whose only inbound edge is `screener … when = "cleared"`.

**Threat-model honesty:** the TCB is host + Wasmtime. It attests integrity/provenance of the composition, not correctness of the screening data, and (because the pipeline is non-deterministic) not output reproducibility.

---

## 8. Demonstration and acceptance

### 8.2 The acceptance moment

> With the screening list seeded to flag a known address, run over a range containing a transfer whose counterparty is that address. **Observe:** the transfer appears in `quarantine` and is **absent** from both the SoR table and the Kafka topic. Then inspect the manifest: `sink-postgres` has **no** `http` capability, and every path to it originates at `screener … when = "cleared"`. The flagged event never reached the writer, and the writer is structurally incapable of fetching it.

### 8.3 Test plan
- Unit per component; integration drop-path; integration fail-closed; integration cache-bust.
- **Attestation test:** parse the manifest; assert `sink-postgres` declares no `http`, and every path to it originates at `screener … when = "cleared"`. Failing this is a compliance regression.

---

## 9. Milestones

- **M0 — W1:** manifest schema + loader + `compose hash`.
- **M1 — W2 + W4:** HTTP origin allow-list; key-value provider with namespace scoping.
- **M2 — W3:** variant-output routing + `when` edges.
- **M3 — W5:** generalized EVM log source + fixture source.
- **M4 — W6:** the seven Customs components.
- **M5 — W7:** offline harness + integration tests.
- **M6 — W8:** `compose sign/verify`, audit-artifact doc, attestation test in CI.

---

## 10. Open questions

1. Conditional-edge expressiveness: start with `when = "<variant-case>"` only.
2. Fail-closed default: hold + durable retry (default) vs halt.
3. Signing scheme: minisign/ed25519 in the example; cosign as production guidance.
4. Key-value backing: in-memory default; Redis for durable `hold`.
5. Backpressure: at-least-once-to-Kafka-after-PG-commit for the example; outbox as production upgrade.
6. Source reorg handling: run behind N confirmations (default 12).

---

## 11. References

- `research.md` — §3.1, §3.2, §3.3, §3.7, §3.8.
- `examples/uni-v3-swaps` — the HTTP-enrichment + multi-sink PoC this builds on.
- WASI 0.2 / `wasi.dev`; Component Model spec; `wasi-keyvalue`; `wasi-http`.
- Wasmtime (`bytecodealliance/wasmtime`).

— Pete
