let db = null;
let latestMeta = null;

let activeFilter = null;

let holderNowPermitIds = [];
let holderHistPermitIds = [];

let exportState = {
  view: "holder-now",
  holderNow: { columns: [], rows: [], filename: "" },
  holderHist: { columns: [], rows: [], filename: "" },
  permitNow: { columns: [], rows: [], filename: "" },
  permitHist: { columns: [], rows: [], filename: "" }
};

// -------------------- UI helpers --------------------
function setStatus(msg) {
  const footer = document.getElementById("statusFooter");
  if (footer) {
    footer.textContent = msg;
    return;
  }
  const legacy = document.getElementById("status");
  if (legacy) legacy.textContent = msg;
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
  exportState.view = tabId;

  document.querySelectorAll(".tab").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
  document.querySelector(`.tab[data-tab="${tabId}"]`).classList.add("active");
  document.getElementById(tabId).classList.add("active");

  updateUrlFromUi();
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

function renderTable(containerId, columns, rows, opts = {}) {
  const { wrapDetailsArea = false, zebra = false } = opts;

  const el = document.getElementById(containerId);
  if (!rows || rows.length === 0) {
    el.innerHTML = "<p>Ingen treff.</p>";
    return;
  }

  let html = "";
  if (wrapDetailsArea) html += `<div class="details-area">`;

  html += `<table${zebra ? ' class="zebra"' : ""}><thead><tr>`;
  for (const c of columns) html += `<th>${escapeHtml(String(c).toUpperCase())}</th>`;
  html += `</tr></thead><tbody>`;

  for (const r of rows) {
    html += `<tr>`;
    for (const cell of r) html += `<td>${cell === null ? "" : escapeHtml(String(cell))}</td>`;
    html += `</tr>`;
  }
  html += `</tbody></table>`;

  if (wrapDetailsArea) html += `</div>`;

  el.innerHTML = html;
}

function renderKeyValue(containerId, obj, title = null) {
  const el = document.getElementById(containerId);
  const keys = Object.keys(obj || {});
  if (!obj || keys.length === 0) {
    el.innerHTML = "<p>Ingen data.</p>";
    return;
  }
  let html = "";
  if (title) html += `<h3>${escapeHtml(title)}</h3>`;
  html += "<table><thead><tr><th>Felt</th><th>Verdi</th></tr></thead><tbody>";
  for (const key of keys) {
    const val = obj[key];
    html += `<tr><td><code>${escapeHtml(key)}</code></td><td>${val == null ? "" : escapeHtml(String(val))}</td></tr>`;
  }
  html += "</tbody></table>";
  el.innerHTML = html;
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
  if (out["TILL_NR"] != null && String(out["TILL_NR"]).trim() !== "") {
    out["TILL_NR"] = formatPermitIdForDisplay(out["TILL_NR"]);
  }
  return out;
}

// -------------------- Clear helpers --------------------
function clearHolderNowUI() {
  holderNowPermitIds = [];
  document.getElementById("holderNowSummary").innerHTML = "";
  document.getElementById("holderNowDetails").innerHTML = "";
  document.getElementById("holderNowDetailsBtn").disabled = true;
  document.getElementById("holderNowExportBtn").disabled = true;
  exportState.holderNow = { columns: [], rows: [], filename: "" };
}
function clearHolderHistUI() {
  holderHistPermitIds = [];
  document.getElementById("holderHistSummary").innerHTML = "";
  document.getElementById("holderHistDetails").innerHTML = "";
  document.getElementById("holderHistDetailsBtn").disabled = true;
  document.getElementById("holderHistExportBtn").disabled = true;
  exportState.holderHist = { columns: [], rows: [], filename: "" };
}
function clearPermitNowUI() {
  document.getElementById("permitNowResult").innerHTML = "";
  document.getElementById("permitNowDetails").innerHTML = "";
  document.getElementById("permitNowExportBtn").disabled = true;
  exportState.permitNow = { columns: [], rows: [], filename: "" };
}
function clearPermitHistUI() {
  document.getElementById("permitHistResult").innerHTML = "";
  document.getElementById("permitHistDetails").innerHTML = "";
  document.getElementById("permitHistExportBtn").disabled = true;
  exportState.permitHist = { columns: [], rows: [], filename: "" };
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
  if (!r.ok) throw new Error(`Klarte ikke å laste DB (HTTP ${r.status})`);
  const gzBuf = new Uint8Array(await r.arrayBuffer());
  const rawBuf = window.pako.ungzip(gzBuf);
  db = new sqlJs.Database(rawBuf);
  setStatus(`Klar. Snapshot: ${latestMeta.snapshot_date}`);
}

function runQuery(sql, params = {}) {
  if (!db) throw new Error("DB ikke lastet ennå");
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

// -------------------- Filter SQL helpers --------------------
function filterConditionSql(permitColumnName = "permit_id") {
  if (!activeFilter) return { sql: "1=1", params: {} };
  return {
    sql: `${permitColumnName} IN (
      SELECT permit_id
      FROM permit_tags
      WHERE snapshot_date = $fsd AND tag = $ftag
    )`,
    params: { $fsd: latestMeta.snapshot_date, $ftag: activeFilter }
  };
}

function countGrunnrenteForPermits(permitIds) {
  if (!permitIds || permitIds.length === 0) return 0;

  const inList = permitIds.map(p => `'${String(p).replaceAll("'", "''")}'`).join(",");
  const q = `
    SELECT COUNT(DISTINCT permit_id)
    FROM permit_tags
    WHERE snapshot_date = $d
      AND tag = 'Grunnrenteskatteplikt'
      AND permit_id IN (${inList})
  `;
  const rows = runQuery(q, { $d: latestMeta.snapshot_date });
  return rows.length ? Number(rows[0][0]) : 0;
}

function permitIsInActiveFilter(permitId) {
  if (!activeFilter) return true;
  const q = `
    SELECT 1
    FROM permit_tags
    WHERE snapshot_date = $d AND tag = $t AND permit_id = $p
    LIMIT 1
  `;
  const rows = runQuery(q, { $d: latestMeta.snapshot_date, $t: activeFilter, $p: permitId });
  return rows.length > 0;
}

// -------------------- URL helpers --------------------
function getCurrentViewId() {
  const activeTab = document.querySelector(".tab.active");
  return activeTab ? activeTab.dataset.tab : "holder-now";
}

function getCurrentIdForView(viewId) {
  if (viewId === "holder-now") return document.getElementById("holderNowInput").value.trim();
  if (viewId === "holder-history") return document.getElementById("holderHistInput").value.trim();
  if (viewId === "permit-now") return document.getElementById("permitNowInput").value.trim();
  if (viewId === "permit-history") return document.getElementById("permitHistInput").value.trim();
  return "";
}

function updateUrlFromUi() {
  const view = getCurrentViewId();
  const id = getCurrentIdForView(view);

  const filter = activeFilter === "Grunnrenteskatteplikt" ? "grunnrente" : "all";

  const params = new URLSearchParams(window.location.search);
  params.set("view", view);
  params.set("filter", filter);

  if (id) params.set("id", id);
  else params.delete("id");

  const newUrl = `${window.location.pathname}?${params.toString()}`;
  window.history.replaceState({}, "", newUrl);
}

function applyUrlParamsIfAny() {
  const params = new URLSearchParams(window.location.search);

  const view = params.get("view");
  const filter = params.get("filter");
  const id = params.get("id");

  if (filter === "grunnrente") activeFilter = "Grunnrenteskatteplikt";
  else activeFilter = null;

  setActiveFilterUi();

  const validViews = new Set(["holder-now", "holder-history", "permit-now", "permit-history"]);
  const finalView = validViews.has(view) ? view : "holder-now";
  switchTab(finalView);

  if (id) {
    if (finalView === "holder-now") document.getElementById("holderNowInput").value = id;
    if (finalView === "holder-history") document.getElementById("holderHistInput").value = id;
    if (finalView === "permit-now") document.getElementById("permitNowInput").value = id;
    if (finalView === "permit-history") document.getElementById("permitHistInput").value = id;

    if (finalView === "holder-now") document.getElementById("holderNowBtn").click();
    if (finalView === "holder-history") document.getElementById("holderHistBtn").click();
    if (finalView === "permit-now") document.getElementById("permitNowBtn").click();
    if (finalView === "permit-history") document.getElementById("permitHistBtn").click();
  } else {
    updateUrlFromUi();
  }
}

// -------------------- CSV helpers --------------------
function csvEscape(value) {
  const s = value == null ? "" : String(value);
  if (/[",\n\r;]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
  return s;
}

function toCsv(columns, rows) {
  const header = columns.map(csvEscape).join(";");
  const lines = rows.map(r => r.map(csvEscape).join(";"));
  return [header, ...lines].join("\n");
}

function downloadCsv(filename, columns, rows) {
  const csv = toCsv(columns, rows);
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();

  URL.revokeObjectURL(url);
}

// -------------------- Snapshot helpers --------------------
function getLatestSnapshotRowJsonForHolder(holderId) {
  const q = `
    SELECT row_json
    FROM permit_snapshot
    WHERE snapshot_date = $d AND holder_id = $h
    LIMIT 1
  `;
  const rows = runQuery(q, { $d: latestMeta.snapshot_date, $h: holderId });
  return rows.length ? rows[0][0] : null;
}

function getLatestSnapshotRowJsonForPermitHolder(permitId, holderId) {
  const q1 = `
    SELECT row_json
    FROM permit_snapshot
    WHERE snapshot_date = $d AND permit_id = $p AND holder_id = $h
    LIMIT 1
  `;
  const r1 = runQuery(q1, { $d: latestMeta.snapshot_date, $p: permitId, $h: holderId });
  if (r1.length) return r1[0][0];

  const q2 = `
    SELECT row_json
    FROM permit_snapshot
    WHERE permit_id = $p AND holder_id = $h
    ORDER BY snapshot_date DESC
    LIMIT 1
  `;
  const r2 = runQuery(q2, { $p: permitId, $h: holderId });
  return r2.length ? r2[0][0] : null;
}

function getSnapshotRowJson(permitId, holderId = null, preferDate = null) {
  if (preferDate) {
    const q1 = holderId
      ? `SELECT row_json FROM permit_snapshot WHERE snapshot_date=$d AND permit_id=$p AND holder_id=$h LIMIT 1`
      : `SELECT row_json FROM permit_snapshot WHERE snapshot_date=$d AND permit_id=$p LIMIT 1`;
    const r1 = runQuery(q1, holderId ? { $d: preferDate, $p: permitId, $h: holderId } : { $d: preferDate, $p: permitId });
    if (r1.length) return r1[0][0];
  }
  const q2 = holderId
    ? `SELECT row_json FROM permit_snapshot WHERE permit_id=$p AND holder_id=$h ORDER BY snapshot_date DESC LIMIT 1`
    : `SELECT row_json FROM permit_snapshot WHERE permit_id=$p ORDER BY snapshot_date DESC LIMIT 1`;
  const r2 = runQuery(q2, holderId ? { $p: permitId, $h: holderId } : { $p: permitId });
  return r2.length ? r2[0][0] : null;
}

function tryExtract(obj, key) {
  const v = obj ? obj[key] : null;
  return v == null ? "" : String(v).trim();
}

// -------------------- Wire inputs to URL updates --------------------
function wireInputClearAndEnter(inputId, onEnterClickId, clearFn) {
  const input = document.getElementById(inputId);
  input.addEventListener("input", (e) => {
    if (e.target.value.trim() === "") clearFn();
    updateUrlFromUi();
  });
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById(onEnterClickId).click();
  });
}

wireInputClearAndEnter("holderNowInput", "holderNowBtn", clearHolderNowUI);
wireInputClearAndEnter("holderHistInput", "holderHistBtn", clearHolderHistUI);
wireInputClearAndEnter("permitNowInput", "permitNowBtn", clearPermitNowUI);
wireInputClearAndEnter("permitHistInput", "permitHistBtn", clearPermitHistUI);

// -------------------- Filter buttons --------------------
document.getElementById("filterAllBtn").addEventListener("click", () => {
  activeFilter = null;
  setActiveFilterUi();
  clearAllSearchUi();
  updateUrlFromUi();
});

document.getElementById("filterGrunnrenteBtn").addEventListener("click", () => {
  activeFilter = "Grunnrenteskatteplikt";
  setActiveFilterUi();
  clearAllSearchUi();
  updateUrlFromUi();
});

// -------------------- CSV buttons --------------------
document.getElementById("holderNowExportBtn").addEventListener("click", () => {
  const st = exportState.holderNow;
  if (!st.columns.length || !st.rows.length) {
    alert("Ingen data å eksportere. Klikk først «Vis detaljer» for å bygge tabellen.");
    return;
  }
  downloadCsv(st.filename || "innehaver_gjeldende.csv", st.columns, st.rows);
});
document.getElementById("holderHistExportBtn").addEventListener("click", () => {
  const st = exportState.holderHist;
  if (!st.columns.length || !st.rows.length) {
    alert("Ingen data å eksportere. Klikk først «Vis detaljer» for å bygge tabellen.");
    return;
  }
  downloadCsv(st.filename || "innehaver_historikk.csv", st.columns, st.rows);
});
document.getElementById("permitNowExportBtn").addEventListener("click", () => {
  const st = exportState.permitNow;
  if (!st.columns.length || !st.rows.length) {
    alert("Ingen data å eksportere. Gjør et søk først.");
    return;
  }
  downloadCsv(st.filename || "tillatelse_eier.csv", st.columns, st.rows);
});
document.getElementById("permitHistExportBtn").addEventListener("click", () => {
  const st = exportState.permitHist;
  if (!st.columns.length || !st.rows.length) {
    alert("Ingen data å eksportere. Gjør et søk først.");
    return;
  }
  downloadCsv(st.filename || "tillatelse_historikk.csv", st.columns, st.rows);
});

// -------------------- Start --------------------
(async function main() {
  try {
    latestMeta = await loadLatestJson();
    const sqlJs = await loadSqlJs();
    await downloadAndOpenDb(sqlJs);

    setActiveFilterUi();
    applyUrlParamsIfAny();
  } catch (e) {
    console.error(e);
    setStatus("Feil: " + e.message);
  }
})();
