import "./style.css";
import { runPipeline, type Routed } from "./host.js";
// Vite ?raw import — the fixture transfers bundled as text.
import fixturesRaw from "./transfers.jsonl?raw";

const REPO = "https://github.com/nightswatchhq/liminal";
const SANCTIONED = "0x722122df12d4e14e13ac3b6895a86e84145b6967";
// The signed offline composition hash (liminal compose hash), v1.0.0.
const COMPOSITION_HASH =
  "86a9f18c3cad42ff1d400306e788ed8c065c47496a59d4b9dd4fbc9eb33a4a19";

const logs = fixturesRaw
  .split("\n")
  .map((s) => s.trim())
  .filter((l) => l && !l.startsWith("#"))
  .map((l) => JSON.parse(l));

const short = (a: string) =>
  a.length > 12 ? `${a.slice(0, 6)}…${a.slice(-4)}` : a;
const usdc = (v: string) => `${(Number(v) / 1e6).toLocaleString()} USDC`;
const sdn = (a: string) =>
  a.toLowerCase() === SANCTIONED ? `<span class="sdn">OFAC-SDN</span>` : "";

function destClass(d: string) {
  if (d === "sink-sor" || d === "sink-kafka") return "sor";
  if (d === "sink-quarantine") return "quar";
  return "hold";
}

function rowHtml(r: Routed): string {
  const dests = r.destinations
    .map((d) => `<span class="dest ${destClass(d)}">${d}</span>`)
    .join(" · ");
  return `<tr>
    <td class="addr">${r.tx}</td>
    <td class="addr">${short(r.from)}${sdn(r.from)}</td>
    <td class="addr">${short(r.to)}${sdn(r.to)}</td>
    <td>${usdc(r.value)}</td>
    <td><span class="badge ${r.verdict}">${r.verdict}</span></td>
    <td>${dests}</td>
  </tr>`;
}

function render(failClosed: boolean) {
  const { rows } = runPipeline(logs, failClosed);
  const flagged = rows.filter((r) => r.verdict === "flagged");
  const flaggedToWriter = flagged.filter((r) => r.reachedWriter).length;
  const written = rows.filter((r) => r.reachedWriter).length;

  const summary = failClosed
    ? `<span class="warn">screening provider DOWN → ${rows.length} transfers held, ${written} written.</span> Fail-closed: nothing reaches the writer.`
    : `<span class="ok">${flaggedToWriter} flagged transfers reached the writer.</span> ${flagged.length} quarantined · ${written} written to the system of record.`;

  document.querySelector("#summary")!.innerHTML = summary;
  document.querySelector("#rows")!.innerHTML = rows.map(rowHtml).join("");
}

const app = document.querySelector<HTMLDivElement>("#app")!;
app.innerHTML = `
  <header>
    <h1>Liminal · Customs</h1>
    <p class="sub">A sanctions-screened transfer indexer — its compiled <b>WASIp2 components run live in your browser</b>.</p>
    <span class="live">real wasm · decoder → screener → enricher, via jco</span>
  </header>

  <div class="panel">
    <h2>The pipeline (capability-isolated DAG)</h2>
    <pre class="dag">fixtures ─▶ <span class="nocap">decoder</span> ─▶ screener
                       ├─ <span class="cleared">cleared</span> ──────▶ enricher ─▶ {sink-sor, sink-kafka}
                       ├─ <span class="flagged">flagged</span> ───────────────────▶ sink-quarantine
                       └─ <span class="hold">indeterminate</span> ─────────────▶ sink-hold   (fail-closed)</pre>
  </div>

  <div class="panel">
    <h2>Run</h2>
    <div class="controls">
      <button id="run">▶ Run pipeline</button>
      <label class="toggle"><input type="checkbox" id="failclosed" /> simulate screening-provider outage</label>
    </div>
    <p id="summary" class="summary" style="margin-top:14px"></p>
    <table>
      <thead><tr><th>tx</th><th>from</th><th>to</th><th>value</th><th>verdict</th><th>routed to</th></tr></thead>
      <tbody id="rows"></tbody>
    </table>
  </div>

  <div class="panel">
    <h2>Why a flagged transfer can't reach the writer</h2>
    <ul class="facts">
      <li><span class="k">Fact 1 —</span> <code>sink-sor</code> (the writer) declares <b>no <code>http</code> capability</b>; by the Component Model it cannot make a network call, by construction.</li>
      <li><span class="k">Fact 2 —</span> the only edge into <code>sink-sor</code> is <code>enricher</code>, whose only inbound edge is <code>screener … when = "cleared"</code>. A flagged verdict has <b>no path</b> to the writer.</li>
      <li class="muted">Both are machine-checked in CI; a change that breaks either is a compliance regression.</li>
    </ul>
    <p class="muted" style="margin-bottom:4px">signed composition (ed25519, content-addressed):</p>
    <div class="hash">sha256:${COMPOSITION_HASH}</div>
  </div>

  <footer>
    These are the actual Rust-compiled WASIp2 components from <a href="${REPO}">${REPO}</a>,
    transpiled with <a href="https://github.com/bytecodealliance/jco">jco</a> and executed in your browser —
    the same routing the native <span class="k">liminal</span> host produces.
    See the <a href="${REPO}/blob/main/examples/customs/RFC.md">RFC</a> and
    <a href="${REPO}/blob/main/examples/customs/AUDIT.md">audit artifact</a>.
  </footer>
`;

const failBox = document.querySelector<HTMLInputElement>("#failclosed")!;
const runBtn = document.querySelector<HTMLButtonElement>("#run")!;

// Visibly clear → "running…" → results on every Run, so the button obviously
// does something even when the routing is unchanged.
function execute() {
  const sumEl = document.querySelector("#summary")!;
  const rowsEl = document.querySelector("#rows")!;
  rowsEl.innerHTML = "";
  sumEl.innerHTML = `<span class="muted">running components…</span>`;
  runBtn.disabled = true;
  setTimeout(() => {
    render(failBox.checked);
    runBtn.disabled = false;
  }, 180);
}

runBtn.addEventListener("click", execute);
failBox.addEventListener("change", execute);
execute();
