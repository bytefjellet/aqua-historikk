let db = null;
let latestMeta = null;

// Aktivt filter: null = alle, ellers tag-navn (f.eks. "Grunnrenteskatteplikt")
let activeFilter = null;

// Holder siste søkeresultat (permitId-lister) så “Vis detaljer” kan bruke dem
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

function renderTable(containerId, columns, rows) {
  const el = document.getElementById(containerId);
  if (!rows || rows.length === 0) {
    el.innerHTML = "<p>Ingen treff.</p>";
    return;
  }
  let html = "<table><thead><tr>";
  for (const c of columns) html += `<th>${escapeHtml(c)}</th>`;
  html += "</tr></thead><tbody>";
  for (const r of rows) {
    html += "<tr>";
    for (const cell of r) html += `<td>${cell === null ? "" : escapeHtml(String(cell))}</td>`;
    html += "</tr>";
  }
  html += "</tbody></table>";
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
  if (!r.ok) throw new Error(`Klarte ikke å laste DB: ${url} (HTTP ${r.status})`);
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
  // Filteret gjelder basert på tag i permit_tags for nyeste snapshot.
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

  // Bygg IN-liste trygt (permitId kan inneholde mellomrom)
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
  // Prøv nyeste snapshotdato først
  const q1 = `
    SELECT row_json
    FROM permit_snapshot
    WHERE snapshot_date = $d AND permit_id = $p AND holder_id = $h
    LIMIT 1
  `;
  const r1 = runQuery(q1, { $d: latestMeta.snapshot_date, $p: permitId, $h: holderId });
  if (r1.length) return r1[0][0];

  // fallback: nyeste tilgjengelige for permit+holder
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

function tryExtract(obj, key) {
  const v = obj ? obj[key] : null;
  return v == null ? "" : String(v).trim();
}

// -------------------- Build holder details table --------------------
function renderHolderDetailsTable(containerId, holderId, permitIds) {
  if (!permitIds || permitIds.length === 0) {
    document.getElementById(containerId).innerHTML = "<p>Ingen tillatelser å vise.</p>";
    return;
  }

  const columns = [
    "TILL_NR",
    "ART",
    "FORMÅL",
    "PRODUKSJONSFORM",
    "TILDELINGSTIDSPUNKT",
    "TILL_KAP",
    "PROD_OMR"
  ];

  const rows = [];
  for (const permitId of permitIds) {
    const rowJson = getLatestSnapshotRowJsonForPermitHolder(permitId, holderId);
    if (!rowJson) continue;

    const obj = JSON.parse(rowJson);

    const art = tryExtract(obj, "ART");
    const formaal = tryExtract(obj, "FORMÅL");
    const prodForm = tryExtract(obj, "PRODUKSJONSFORM");
    const tildeling = tryExtract(obj, "TILDELINGSTIDSPUNKT");
    const kap = tryExtract(obj, "TILL_KAP");
    const enh = tryExtract(obj, "TILL_ENHET");
    const kapStr = [kap, enh].filter(Boolean).join(" ").trim();
    const prodOmr = tryExtract(obj, "PROD_OMR");

    rows.push([
      permitId,
      art,
      formaal,
      prodForm,
      tildeling,
      kapStr,
      prodOmr
    ]);
  }

  // Sorter på TILL_NR for stabil visning
  rows.sort((a, b) => String(a[0]).localeCompare(String(b[0]), "no"));

  renderTable(containerId, columns, rows);
}

// -------------------- View: Innehaver i dag --------------------
document.getElementById("holderNowBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderNowInput").value.trim();
  if (!holderId) { clearHolderNowUI(); return; }

  // Hent aktive permits (respekterer aktivt UI-filter for selve “visningen”)
  const fc = filterConditionSql("permit_id");
  const q
