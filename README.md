# Liminal

[![CI](https://github.com/nuthatch-org/liminal/actions/workflows/ci.yml/badge.svg)](https://github.com/nuthatch-org/liminal/actions/workflows/ci.yml)

**Polyglot, capability-isolated WASIp2 component runtime for streaming indexing pipelines on The Graph.**

> 🌐 **Live browser demo:** [**liminal-customs.vercel.app**](https://liminal-customs.vercel.app) — the actual
> compiled Customs WASIp2 components (decoder → screener → enricher) run **in your browser** via
> [jco](https://github.com/bytecodealliance/jco), producing the same routing as the native host.
> Watch flagged transfers get barred from the writer; toggle a screening outage to see fail-closed.

Liminal is a third lane alongside Subgraphs and Substreams — not a replacement for either.

- **Use Subgraphs** when you need a GraphQL endpoint served by the decentralised network with Proof-of-Indexing.
- **Use Substreams** when you need high-throughput parallel historical backfill of pure-compute transformations.
- **Use Liminal** when your pipeline needs effectful middleware, capability-isolated plugins, polyglot stages, mid-block emission, hot-swap without source resync, or multiple sinks off a single cursor.

---

## Why Liminal

The pipelines Liminal targets exist today — they're just built by gluing four systems together:

- a Substreams or Subgraph for chain-side decoding
- a custom sink for the effectful step
- a sidecar service for whatever the sink can't do
- a reconciliation layer because cursors now live in three places

Liminal replaces that stack with a single WASIp2 component pipeline: one cursor, one process, one observable failure boundary. Capabilities (HTTP, key-value, filesystem) are granted per-component by the host, not per-pipeline — so untrusted middleware is a concrete engineering construct, not a code-review prayer.

Full rationale: [research.md](./research.md)

---

## How it works

A Liminal pipeline is a **DAG declared in a manifest**. The host is generic: it reads the
manifest, instantiates each component into its own capability-scoped sandbox, wires the graph,
and streams source messages through. Adding a new pipeline shape is a config change — the host
is never recompiled.

```
source ──▶ decoder ──▶ enricher ──▶ sink ──▶ (dashboard / db / queue)
   (bytes)     (bytes)      (bytes)
```

Every component — whatever it does — exports **one** function:

```wit
// wit/world.wit
interface node {
    transform: func(input: list<u8>) -> result<list<list<u8>>, string>;
}
```

The host pipes opaque bytes (JSON by convention) along the edges, blind to the payload. That
blindness is the point: the runtime moves bytes; the components give them meaning.

### The manifest

```toml
name = "cross-dex-arb"

[source]
type = "evm"
rpc = "${ETH_RPC_URL}"          # ${VAR} and ${VAR:-default} are interpolated
topics = ["0xc42079…", "0x2170c7…"]

[[nodes]]
id = "decoder"
wasm = "examples/cross-dex-arb/decoder.wasm"
# no capabilities → pure compute, cannot touch the network even if it tried

[[nodes]]
id = "enricher"
wasm = "examples/cross-dex-arb/enricher.wasm"
capabilities = ["http"]          # the ONLY node granted network egress
env = { ORACLE_URL = "https://coins.llama.fi" }

[[nodes]]
id = "sink"
wasm = "examples/cross-dex-arb/sink-json.wasm"
capabilities = ["stdout"]

[[edges]]
from = "source"
to = "decoder"
[[edges]]
from = "decoder"
to = "enricher"
[[edges]]
from = "enricher"
to = "sink"

[dashboard]                      # optional: terminal-node output → live SSE
port = 8080
html = "examples/cross-dex-arb/dashboard/index.html"
```

### Capability isolation, enforced by construction

Capabilities are **data**, granted per node. A node that imports a capability it wasn't granted
**fails to instantiate** — loudly, at load time. This isn't a convention; it's a test:

```
test runtime::tests::http_capability_is_enforced_at_load ... ok
```

The enricher imports `wasi:http`. It loads with `capabilities = ["http"]` and is refused
without it. The decoder and sinks have no network grant and physically cannot make an outbound
request.

### Authoring a component

A component is a typed closure. The `liminal-sdk` `node!` macro handles the WIT boundary and
JSON (de)serialisation — there is no bindgen boilerplate to write:

```rust
use liminal_sdk::{node, EvmLog};
use arb_types::NormalizedSwap;

node!(|log: EvmLog| -> Result<Vec<NormalizedSwap>, String> {
    // Ok(vec![])      → filter this message
    // Ok(vec![swap])  → emit downstream
    // Err(msg)        → recoverable per-message error (host logs, carries on)
    Ok(decode(log).into_iter().collect())
});
```

---

## Workspace

```
liminal/
├── liminal-host/           # Generic manifest-driven runtime (the `liminal` binary)
│   ├── manifest.rs         #   TOML manifest: parse, ${env} interpolation, DAG validation
│   ├── runtime.rs          #   load nodes (capability-scoped), wire DAG, conditional routing
│   ├── source.rs           #   EVM WebSocket log source + offline fixture source → bytes
│   ├── http_policy.rs      #   W2: per-node HTTP origin allow-list (wasi:http hook)
│   ├── kv_host.rs          #   W4: namespaced liminal:kv/store provider
│   ├── compose.rs          #   W1+/W8: content addressing + ed25519 sign/verify
│   ├── node_bindings.rs    #   host bindgen for the universal node interface
│   └── dashboard.rs        #   optional SSE dashboard fed by terminal-node output
├── liminal-sdk/            # `node!` / `node_kv!` macros + shared EvmLog wire type
├── wit/world.wit           # The universal node interface + the kv store interface
├── justfile                # build / test / run / verify recipes
├── .github/workflows/ci.yml # build + test + `compose verify` on every push
└── examples/
    ├── uni-v3-swaps/        # decoder → enricher → Postgres + Kafka fan-out
    │   ├── types/           #   shared serde wire types for this pipeline
    │   ├── {decoder,price-enricher,sink-postgres,sink-kafka}/
    │   └── pipeline.toml
    ├── cross-dex-arb/       # Uni v3 + Balancer v2 → live arb dashboard
    │   ├── types/
    │   ├── {decoder,enricher,sink-json}/
    │   ├── dashboard/index.html
    │   └── pipeline.toml
    └── customs/             # ★ compliance-gated transfer indexer (RFC-LIM-001)
        ├── RFC.md  AUDIT.md  README.md
        ├── types/
        ├── {decoder,screener,screener-http,enricher}/
        ├── {sink-sor,sink-kafka,sink-quarantine,sink-hold}/
        ├── screening-server/        #   local sanctions provider (axum)
        ├── fixtures/                #   transfers.jsonl + sanctioned.json
        ├── customs.pipeline.toml    #   offline · + .sig + customs.pub (signed)
        ├── customs.live.pipeline.toml
        ├── docker-compose.yml  run.sh
```

---

## Building

```bash
# host (native) — note `cargo build` alone builds only natively-linkable crates
cargo build --release -p liminal-host

# components (wasm) + stage artifacts — or just `just build` for everything
just build
```

`just build` builds the host and every component (all three pipelines) for `wasm32-wasip2` and
copies the `.wasm` files next to their manifests. (Plain `cargo build` deliberately skips the
component crates — they're cdylibs with WIT exports that only link for the wasm target.)

---

## Running

Every pipeline is just a manifest handed to the same generic binary:

```bash
# Customs compliance demo — fully offline, no RPC or services needed
just run-customs
#   flagged transfers → quarantine only; cleared → SoR + Kafka; unresolvable → hold

# Customs LIVE — starts the local screening-server; screener calls it over
# origin-scoped wasi:http. Stop the server and re-run to see it fail closed.
just run-customs-live

# Verify the signed Customs composition (content addresses + topology + caps)
just verify-customs

# --- the mainnet examples (need a node) ---
export ETH_RPC_URL=wss://your-node

# Cross-DEX arbitrage tracker → dashboard at http://localhost:8080
just run-arb
#   …or: cargo run --release -p liminal-host -- run examples/cross-dex-arb/pipeline.toml

# Uniswap v3 swaps → Postgres + Kafka fan-out (stop after 5 messages)
just run-uni --limit 5
```

### Composition attestation (`compose`)

Beyond running pipelines, the `liminal` binary can hash, sign, and verify a composition — the
provenance story behind Customs (the topology is *signed*):

```bash
liminal compose hash   examples/customs/customs.pipeline.toml      # content-address each component + canonical hash
liminal compose keygen examples/customs/customs                    # → customs.key (secret), customs.pub
liminal compose sign   examples/customs/customs.pipeline.toml --key examples/customs/customs.key
liminal compose verify examples/customs/customs.pipeline.toml \
    --sig examples/customs/customs.pipeline.toml.sig --pub examples/customs/customs.pub
```

The signed body is the **canonical composition** — component ids + the `sha256` of their actual
wasm bytes + capability declarations + the edge set + the structural source filter — with every
`${VAR}` runtime secret excluded. The same signature therefore validates in staging and prod with
different endpoints; it attests *structure and capability boundaries*, not config.

`DATABASE_URL` and `KAFKA_BROKERS` are optional — the uni-v3 manifest supplies `${VAR:-default}`
fallbacks, and the PoC sinks emit SQL / JSON to stdout rather than connecting to live services.

---

## Status

A **generic, manifest-driven, capability-isolated runtime** with three pipelines expressed
entirely as config — including **Customs**, a compliance-grade indexer (RFC-LIM-001, complete).

- **One interface** (`transform: bytes -> [bytes]`) for every component.
- **Pipelines are data** — `pipeline.toml`, not host code; conditional `when` routing on a verdict.
- **Capabilities are data** — granted per node and enforced: load-time grant (no import ⇒ no access),
  HTTP origin allow-lists, and namespaced key-value isolation.
- **Compositions are signed** — content-addressed (`sha256` per component) + ed25519; `compose verify`
  runs in CI as a compliance gate.
- **`node!` / `node_kv!` SDK macros** — components are typed closures, zero bindgen boilerplate.
- `cargo build` is clean; **13 tests** cover manifest validation, capability enforcement, KV namespace
  isolation, HTTP origin policy, sign/verify, and the Customs compliance properties (drop-path +
  fail-closed). Green CI on every push.

Built on Wasmtime 44 (WASIp2 / WASI 0.2.x). WASIp3 (async streams, structured concurrency) is
tracked upstream; Liminal will migrate once it stabilises in Wasmtime.

---

## Customs (RFC-LIM-001) — ✅ complete

[**Customs**](./examples/customs/RFC.md) is the first *compliance-grade* pipeline: a
sanctions-screened ERC-20 transfer indexer that proves the one thing no Subgraph + Substreams +
sidecar stack can express — **architectural non-ingestion under capability isolation**. A flagged
transfer is routed to quarantine and is *structurally* incapable of reaching the system-of-record
writer, because the writer has no edge from the flagged branch and imports no HTTP. The compliance
control is the topology, and the topology is signed. See [`AUDIT.md`](./examples/customs/AUDIT.md)
for the auditor's two-fact attestation.

```
fixtures / evm-logs ─▶ decoder ─▶ screener
                                    ├─ cleared ──────▶ enricher ─▶ {sink-sor, sink-kafka}
                                    ├─ flagged ───────────────────▶ sink-quarantine
                                    └─ indeterminate ─────────────▶ sink-hold   (fail-closed)
```

All eight workstreams (W) and seven milestones (M) are shipped; the checklist below is the record,
with the two deliberately-deferred residuals marked.

### Platform deltas (reusable — every future effectful pipeline needs these)

- [x] **W1 — Declarative manifest + loader** (`pipeline.toml`, `${VAR}` interpolation, DAG validation) — *shipped in v0.2*
- [x] **W1+ — Content addressing** (`sha256` per component; `liminal compose hash` canonical composition hash)
- [x] **W3 — Conditional routing** (`when = "<case>"` edges; host dispatches on the output `"tag"` discriminant)
- [x] **W5 — Source generalization** (EVM `topic0` + address filter; offline `fixture` source)
- [x] **W8 — Compose signing/verification** (`liminal compose keygen|sign|verify`, ed25519; cosign as production guidance) + [`AUDIT.md`](./examples/customs/AUDIT.md) + CI runs `compose verify` on every push
- [x] **W2 — HTTP origin allow-list** (host-enforced `allow_origins` on `wasi:http/outgoing-handler`; in the canonical signed body)
- [x] **W4 — key-value with namespace scoping** (host-provided `liminal:kv/store`, hand-rolled on Wasmtime 44; per-node namespace, isolation enforced + tested; `node_kv!` SDK macro; screener caches verdicts). *Migrating to the standard `wasi:keyvalue` + Redis remains a deliberate Wasmtime-45 bump.*

### Customs application & harness

- [x] **W6 — Components** — `decoder` · `screener` · `enricher` · `sink-sor` (no HTTP) · `sink-kafka` · `sink-quarantine` · `sink-hold`
- [x] **W6 — `customs.pipeline.toml`** manifest with `when` edges
- [x] **W6 — Attestation test** — parses the manifest; asserts `sink-sor` has no `http` capability and every path to it originates at `screener … when = "cleared"`. *Failing this is a compliance regression.*
- [x] **W7 — Offline run** — `fixtures/transfers.jsonl` + `just run-customs`, fully offline (no RPC/services)
- [x] **W7 — Drop-path integration test** — flagged → quarantine, **absent** from SoR/Kafka; indeterminate → hold (`tests/customs_e2e.rs`)
- [x] **W7 — Live harness** — `screening-server` (axum) + `screener-http` calling it over origin-scoped `wasi:http`; `just run-customs-live`; `docker-compose.yml` for pg/kafka/redis
- [x] **W7 — Fail-closed integration test** — provider unreachable ⇒ every transfer held, nothing written (`unreachable_provider_holds_everything`)
- [ ] **W7 — Residual** — Redis-backed *durable* hold (cross-restart) + a cross-run cache-bust test; both want the deliberate Wasmtime-45 / `wasi:keyvalue` + Redis bump

### Milestones

| M | Workstreams | Status | Outcome |
|---|---|---|---|
| **M0** | W1 + W1+ | ✅ | Manifest schema + loader + content addressing + `compose hash` |
| **M1** | W2 + W4 | ✅ | HTTP origin allow-lists + namespaced key-value (standard `wasi:keyvalue`/Redis on the 45 bump) |
| **M2** | W3 | ✅ | Variant-output routing + `when` edges (**the centerpiece**) |
| **M3** | W5 | ✅ | Generalized EVM source + offline fixture source |
| **M4** | W6 | ✅ | The seven Customs components + manifest + attestation test |
| **M5** | W7 | ✅ | Offline + live (`screening-server`) harness; drop-path + fail-closed tests (Redis durable hold = residual) |
| **M6** | W8 | ✅ | `compose keygen/sign/verify`, `AUDIT.md` audit-artifact doc, CI gates on `compose verify` |

### General platform backlog (not Customs-specific)

- [ ] Durable cursor / checkpointing for resume-after-restart and hot-swap without resync.
- [ ] Concurrent fan-out across sibling branches (today the DAG is walked breadth-first per message).
- [ ] WASIp3 migration (mid-block `stream<T>` emission) once it stabilises in Wasmtime.

---

## References
- [RFC-LIM-001 — Customs](./examples/customs/RFC.md) · [Customs README](./examples/customs/README.md) · [AUDIT.md](./examples/customs/AUDIT.md)
- [WASI 0.2 / wasi.dev](https://wasi.dev)
- [Wasmtime](https://github.com/bytecodealliance/wasmtime)
- [Component Model spec](https://github.com/WebAssembly/component-model)
