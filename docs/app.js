let db = null;
let latestMeta = null;

let activeFilter = null;

let holderNowPermitIds = [];
let holderHistPermitIds = [];

// -------------------- UI helpers --------------------
function setStatus(msg) {
  document.getElementById("status").textContent = msg;
}

function setActiveFilterUi() {
  const allBtn = document.getElementById("filterAllBtn");
  const grBtn = document.getElementById("filterGrunnrenteBtn");
  const label = document.getElementById("activeFilterLabel");

  allBtn.classList.toggle("active", activeFilter === null);
  grBtn.classList.toggle("active", activeFilter === "Grunnrenteskatteplikt");

  label.textContent = activeFilter ? `Aktivt filter: ${activeFilter}` : "Aktivt filter: Alle";
}

function switchTab(tabId) {
  document.querySelectorAll(".tab").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
  document.querySelector(`.tab[data-tab="${tabId}"]`).classList.add("active");
  document.getElementById(tabId).classList.add("active");
}

document.querySelectorAll(".tab").forEach(btn => {
  btn.addEventListener("click", () => switchTab(btn.dataset.tab));
});

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

// -------------------- Formatting helpers --------------------
function formatPermitIdForDisplay(permitId) {
  return String(permitId).trim().replace(/\s+/g, "-");
}

function normalizePermitIdForLookup(input) {
  if (!input) return "";
  return String(input)
    .trim()
    .replace(/\s*-\s*/g, " ")
    .replace(/\s+/g, " ");
}

function normalizeDetailsObjectForDisplay(obj) {
  if (!obj || typeof obj !== "object") return obj;
  const out = { ...obj };
  if (out["TILL_NR"]) out["TILL_NR"] = formatPermitIdForDisplay(out["TILL_NR"]);
  return out;
}

// -------------------- Clear helpers --------------------
function clearHolderNowUI() {
  holderNowPermitIds = [];
  document.getElementById("holderNowSummary").innerHTML = "";
  document.getElementById("holderNowDetails").innerHTML = "";
  document.getElementById("holderNowDetailsBtn").disabled = true;
}
function clearHolderHistUI() {
  holderHistPermitIds = [];
  document.getElementById("holderHistSummary").innerHTML = "";
  document.getElementById("holderHistDetails").innerHTML = "";
  document.getElementById("holderHistDetailsBtn").disabled = true;
}
function clearPermitNowUI() {
  document.getElementById("permitNowResult").innerHTML = "";
  document.getElementById("permitNowDetails").innerHTML = "";
}
function clearPermitHistUI() {
  document.getElementById("permitHistResult").innerHTML = "";
  document.getElementById("permitHistDetails").innerHTML = "";
}
function clearAllSearchUi() {
  clearHolderNowUI();
  clearHolderHistUI();
  clearPermitNowUI();
  clearPermitHistUI();
}

// -------------------- DB loading --------------------
async function loadLatestJson() {
  const r = await fetch("./latest.json", { cache: "no-store" });
  if (!r.ok) throw new Error("Klarte ikke å laste latest.json");
  return await r.json();
}

async function loadSqlJs() {
  return await initSqlJs({
    locateFile: (file) => `https://cdn.jsdelivr.net/npm/sql.js@1.10.3/dist/${file}`
  });
}

async function downloadAndOpenDb(sqlJs) {
  const url = latestMeta.sqlite_gz_url;
  setStatus(`Laster database (${latestMeta.snapshot_date})…`);
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`Klarte ikke å laste DB`);
  const gzBuf = new Uint8Array(await r.arrayBuffer());
  const rawBuf = window.pako.ungzip(gzBuf);
  db = new sqlJs.Database(rawBuf);
  setStatus(`Klar. Snapshot: ${latestMeta.snapshot_date}`);
}

function runQuery(sql, params = {}) {
  const stmt = db.prepare(sql);
  try {
    stmt.bind(params);
    const rows = [];
    while (stmt.step()) rows.push(stmt.get());
    return rows;
  } finally {
    stmt.free();
  }
}

// -------------------- Render helpers --------------------
function renderTable(containerId, columns, rows) {
  const el = document.getElementById(containerId);
  if (!rows.length) {
    el.innerHTML = "<p>Ingen treff.</p>";
    return;
  }
  let html = `<div class="details-area"><table class="zebra"><thead><tr>`;
  for (const c of columns) html += `<th>${escapeHtml(c)}</th>`;
  html += `</tr></thead><tbody>`;
  for (const r of rows) {
    html += "<tr>";
    for (const cell of r) html += `<td>${escapeHtml(cell ?? "")}</td>`;
    html += "</tr>";
  }
  html += "</tbody></table></div>";
  el.innerHTML = html;
}

function renderKeyValueTableHtml(obj, title) {
  const o = normalizeDetailsObjectForDisplay(obj);
  let html = `<h4>${escapeHtml(title)}</h4>`;
  html += `<div class="details-area"><table class="details-kv zebra"><thead>
    <tr><th>Felt</th><th>Verdi</th></tr></thead><tbody>`;
  for (const k of Object.keys(o).sort()) {
    html += `<tr><td><code>${escapeHtml(k)}</code></td><td>${escapeHtml(o[k] ?? "")}</td></tr>`;
  }
  html += `</tbody></table></div>`;
  return html;
}

// -------------------- Cards --------------------
function cardFromSummary(summaryObj, permitId, holderId, date, targetId, heading) {
  const btn = `
    <button class="details-btn"
      data-target="${targetId}"
      data-permit="${permitId}"
      data-holder="${holderId || ""}"
      data-date="${date || ""}">
      Vis detaljer
    </button>
  `;
  let html = `<h4>${escapeHtml(heading)}</h4>`;
  html += `<div class="row">${btn}</div>`;
  html += renderKeyValueTableHtml(summaryObj, "Oppsummering");
  return html;
}

// -------------------- Global handler for “Vis detaljer” --------------------
document.addEventListener("click", (e) => {
  const btn = e.target.closest(".details-btn");
  if (!btn) return;

  const target = btn.dataset.target;
  const permitId = btn.dataset.permit;
  const holderId = btn.dataset.holder || null;
  const date = btn.dataset.date || null;

  const row = getSnapshotRowJson(permitId, holderId, date);
  if (!row) return;

  const display = formatPermitIdForDisplay(permitId);
  document.getElementById(target).innerHTML =
    renderKeyValueTableHtml(JSON.parse(row), `Vis detaljer for ${display}`);
});

// -------------------- Start --------------------
(async function main() {
  try {
    latestMeta = await loadLatestJson();
    const sqlJs = await loadSqlJs();
    await downloadAndOpenDb(sqlJs);
    setActiveFilterUi();
  } catch (e) {
    setStatus("Feil: " + e.message);
  }
})();
