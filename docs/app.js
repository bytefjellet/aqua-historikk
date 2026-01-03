let db = null;
let latestMeta = null;

// Aktivt filter: null = alle, ellers tag-navn (f.eks. "Grunnrenteskatteplikt")
let activeFilter = null;

// Holder siste søkeresultat (permitId-lister) så “Vis detaljer” kan bruke dem (disse følger aktivt filter)
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
  // Kapitaliser overskrifter (i tillegg til CSS)
  for (const c of columns) html += `<th>${escapeHtml(String(c).toUpperCase())}</th>`;
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

// -------------------- STATUS helper --------------------
function getPermitStatusForHolder(holderId, permitId) {
  const qActive = `
    SELECT 1
    FROM ownership_intervals
    WHERE holder_id = $h AND permit_id = $p AND valid_to IS NULL
    LIMIT 1
  `;
  const a = runQuery(qActive, { $h: holderId, $p: permitId });
  if (a.length > 0) return "Aktiv";

  const qEnded = `
    SELECT valid_to
    FROM ownership_intervals
    WHERE holder_id = $h AND permit_id = $p AND valid_to IS NOT NULL
    ORDER BY valid_to DESC
    LIMIT 1
  `;
  const e = runQuery(qEnded, { $h: holderId, $p: permitId });
  if (e.length > 0 && e[0][0]) return `Avsluttet (til ${e[0][0]})`;

  return "Ukjent";
}

// -------------------- Build holder details table --------------------
function renderHolderDetailsTable(containerId, holderId, permitIds) {
  if (!permitIds || permitIds.length === 0) {
    document.getElementById(containerId).innerHTML = "<p>Ingen tillatelser å vise.</p>";
    return;
  }

  const columns = [
    "STATUS",
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
    if (!rowJson) {
      rows.push([
        getPermitStatusForHolder(holderId, permitId),
        permitId, "", "", "", "", "", ""
      ]);
      continue;
    }

    const obj = JSON.parse(rowJson);

    const kap = tryExtract(obj, "TILL_KAP");
    const enh = tryExtract(obj, "TILL_ENHET");
    const kapStr = [kap, enh].filter(Boolean).join(" ").trim();

    rows.push([
      getPermitStatusForHolder(holderId, permitId),
      permitId,
      tryExtract(obj, "ART"),
      tryExtract(obj, "FORMÅL"),
      tryExtract(obj, "PRODUKSJONSFORM"),
      tryExtract(obj, "TILDELINGSTIDSPUNKT"),
      kapStr,
      tryExtract(obj, "PROD_OMR")
    ]);
  }

  const statusRank = (s) => (String(s).startsWith("Aktiv") ? 0 : 1);
  rows.sort((a, b) => {
    const ra = statusRank(a[0]);
    const rb = statusRank(b[0]);
    if (ra !== rb) return ra - rb;
    return String(a[1]).localeCompare(String(b[1]), "no");
  });

  renderTable(containerId, columns, rows);
}

// -------------------- View: Innehaver i dag --------------------
document.getElementById("holderNowBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderNowInput").value.trim();
  if (!holderId) { clearHolderNowUI(); return; }

  // 1) Total (uten filter): aktive permits
  const qAll = `
    SELECT DISTINCT permit_id
    FROM ownership_intervals
    WHERE holder_id = $h AND valid_to IS NULL
    ORDER BY permit_id
  `;
  const allRows = runQuery(qAll, { $h: holderId });
  const allPermitIds = allRows.map(r => r[0]);

  // 2) Visning (med aktivt filter): aktive permits
  const fc = filterConditionSql("permit_id");
  const qView = `
    SELECT DISTINCT permit_id
    FROM ownership_intervals
    WHERE holder_id = $h AND valid_to IS NULL
      AND ${fc.sql}
    ORDER BY permit_id
  `;
  const viewRows = runQuery(qView, { $h: holderId, ...fc.params });
  holderNowPermitIds = viewRows.map(r => r[0]);

  if (allPermitIds.length === 0) {
    document.getElementById("holderNowSummary").innerHTML = "<p>Ingen aktive tillatelser funnet for denne innehaveren.</p>";
    document.getElementById("holderNowDetails").innerHTML = "";
    document.getElementById("holderNowDetailsBtn").disabled = true;
    return;
  }

  let navn = "";
  const anyRowJson = getLatestSnapshotRowJsonForHolder(holderId);
  if (anyRowJson) {
    const obj = JSON.parse(anyRowJson);
    navn = tryExtract(obj, "NAVN");
  }

  const totalCount = allPermitIds.length;
  const grunnrenteCount = countGrunnrenteForPermits(allPermitIds);

  // Ingen "antall i visning" (som du ønsket)
  renderKeyValue("holderNowSummary", {
    "ORG.NR/PERS.NR": holderId,
    "Selskapsnavn": navn || "(mangler)",
    "Antall tillatelser (totalt)": totalCount,
    "Antall tillatelser (Grunnrenteskatteplikt)": grunnrenteCount
  });

  document.getElementById("holderNowDetails").innerHTML = "";
  document.getElementById("holderNowDetailsBtn").disabled = (activeFilter && holderNowPermitIds.length === 0);
});

document.getElementById("holderNowDetailsBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderNowInput").value.trim();
  if (!holderId) { clearHolderNowUI(); return; }
  renderHolderDetailsTable("holderNowDetails", holderId, holderNowPermitIds);
});

// -------------------- View: Innehaver historikk --------------------
document.getElementById("holderHistBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderHistInput").value.trim();
  if (!holderId) { clearHolderHistUI(); return; }

  // 1) Total (uten filter): unike permits over tid
  const qAll = `
    SELECT DISTINCT permit_id
    FROM ownership_intervals
    WHERE holder_id = $h
    ORDER BY permit_id
  `;
  const allRows = runQuery(qAll, { $h: holderId });
  const allPermitIds = allRows.map(r => r[0]);

  // 2) Visning (med aktivt filter)
  const fc = filterConditionSql("permit_id");
  const qView = `
    SELECT DISTINCT permit_id
    FROM ownership_intervals
    WHERE holder_id = $h
      AND ${fc.sql}
    ORDER BY permit_id
  `;
  const viewRows = runQuery(qView, { $h: holderId, ...fc.params });
  holderHistPermitIds = viewRows.map(r => r[0]);

  if (allPermitIds.length === 0) {
    document.getElementById("holderHistSummary").innerHTML = "<p>Ingen historikk funnet for denne innehaveren.</p>";
    document.getElementById("holderHistDetails").innerHTML = "";
    document.getElementById("holderHistDetailsBtn").disabled = true;
    return;
  }

  let navn = "";
  const anyRowJson = getLatestSnapshotRowJsonForHolder(holderId);
  if (anyRowJson) {
    const obj = JSON.parse(anyRowJson);
    navn = tryExtract(obj, "NAVN");
  }

  const totalCount = allPermitIds.length;
  const grunnrenteCount = countGrunnrenteForPermits(allPermitIds);

  // Ingen "antall i visning" (som du ønsket)
  renderKeyValue("holderHistSummary", {
    "ORG.NR/PERS.NR": holderId,
    "Selskapsnavn": navn || "(mangler)",
    "Antall tillatelser (totalt)": totalCount,
    "Antall tillatelser (Grunnrenteskatteplikt)": grunnrenteCount
  });

  document.getElementById("holderHistDetails").innerHTML = "";
  document.getElementById("holderHistDetailsBtn").disabled = (activeFilter && holderHistPermitIds.length === 0);
});

document.getElementById("holderHistDetailsBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderHistInput").value.trim();
  if (!holderId) { clearHolderHistUI(); return; }
  renderHolderDetailsTable("holderHistDetails", holderId, holderHistPermitIds);
});

// -------------------- Permit views (uendret fra tidligere) --------------------
function renderKeyValueTableHtml(obj, title = null) {
  const keys = Object.keys(obj || {});
  if (!obj || keys.length === 0) return "<p>Ingen data.</p>";
  let html = "";
  if (title) html += `<h4>${escapeHtml(title)}</h4>`;
  html += "<table><thead><tr><th>Felt</th><th>Verdi</th></tr></thead><tbody>";
  for (const key of keys.sort()) {
    const val = obj[key];
    html += `<tr><td><code>${escapeHtml(key)}</code></td><td>${val == null ? "" : escapeHtml(String(val))}</td></tr>`;
  }
  html += "</tbody></table>";
  return html;
}
function renderVerticalCards(containerId, cardsHtml) {
  const el = document.getElementById(containerId);
  if (!cardsHtml || cardsHtml.length === 0) { el.innerHTML = "<p>Ingen treff.</p>"; return; }
  el.innerHTML = cardsHtml.join("\n<hr />\n");
}
function cardFromSummary(summaryObj, permitId, holderId, preferDate, detailsTargetId, heading) {
  const detailsBtn = `
    <button class="details-btn"
      data-target="${escapeHtml(detailsTargetId)}"
      data-permit="${escapeHtml(permitId)}"
      data-holder="${escapeHtml(holderId || "")}"
      data-date="${escapeHtml(preferDate || "")}">
      Detaljer
    </button>
  `;
  const title = heading ? `<h4>${escapeHtml(heading)}</h4>` : "";
  const kv = renderKeyValueTableHtml(summaryObj);
  return `${title}<div class="row">${detailsBtn}</div>${kv}`;
}

document.getElementById("permitNowBtn").addEventListener("click", () => {
  const permitId = document.getElementById("permitNowInput").value.trim();
  if (!permitId) { clearPermitNowUI(); return; }

  if (activeFilter && !permitIsInActiveFilter(permitId)) {
    document.getElementById("permitNowResult").innerHTML =
      `<p>Tillatelsen <code>${escapeHtml(permitId)}</code> er ikke innen filteret “${escapeHtml(activeFilter)}”.</p>`;
    document.getElementById("permitNowDetails").innerHTML = "";
    return;
  }

  const rowJson = getSnapshotRowJson(permitId, null, latestMeta.snapshot_date);
  if (!rowJson) {
    document.getElementById("permitNowResult").innerHTML = "<p>Fant ingen tillatelse med dette nummeret i snapshot-tabellen.</p>";
    document.getElementById("permitNowDetails").innerHTML = "";
    return;
  }
  const obj = JSON.parse(rowJson);

  const qOwner = `
    SELECT holder_id, valid_from
    FROM ownership_intervals
    WHERE permit_id=$p AND valid_to IS NULL
    ORDER BY valid_from DESC
    LIMIT 1
  `;
  const own = runQuery(qOwner, { $p: permitId });
  const eier = own.length ? own[0][0] : "";
  const eierFra = own.length ? own[0][1] : "";

  const summary = {
    "TILL_NR": permitId,
    "NAVN": tryExtract(obj, "NAVN") || "(mangler)",
    "Snapshot": latestMeta.snapshot_date,
    "Eier (fra historikk-tabell)": eier ? `${eier} (fra ${eierFra})` : "(ikke funnet)"
  };

  renderVerticalCards("permitNowResult", [
    cardFromSummary(summary, permitId, eier || "", latestMeta.snapshot_date, "permitNowDetails", `Tillatelse ${permitId}`)
  ]);
  document.getElementById("permitNowDetails").innerHTML = "";
});

document.getElementById("permitHistBtn").addEventListener("click", () => {
  const permitId = document.getElementById("permitHistInput").value.trim();
  if (!permitId) { clearPermitHistUI(); return; }

  if (activeFilter && !permitIsInActiveFilter(permitId)) {
    document.getElementById("permitHistResult").innerHTML =
      `<p>Tillatelsen <code>${escapeHtml(permitId)}</code> er ikke innen filteret “${escapeHtml(activeFilter)}”.</p>`;
    document.getElementById("permitHistDetails").innerHTML = "";
    return;
  }

  const q = `
    SELECT holder_id, valid_from, COALESCE(valid_to, 'NÅ') AS valid_to
    FROM ownership_intervals
    WHERE permit_id=$p
    ORDER BY valid_from
  `;
  const rows = runQuery(q, { $p: permitId });

  if (!rows.length) {
    document.getElementById("permitHistResult").innerHTML = "<p>Ingen historikk funnet for denne tillatelsen.</p>";
    document.getElementById("permitHistDetails").innerHTML = "";
    return;
  }

  const cards = rows.map(([holderId, from, to]) => {
    const rowJson = getSnapshotRowJson(permitId, holderId, from);
    let navn = "";
    if (rowJson) {
      const obj = JSON.parse(rowJson);
      navn = tryExtract(obj, "NAVN");
    }
    const summary = {
      "TILL_NR": permitId,
      "ORG.NR/PERS.NR": holderId,
      "NAVN": navn || "(mangler)",
      "Fra": from,
      "Til": to
    };
    return cardFromSummary(summary, permitId, holderId, from, "permitHistDetails", `Eierperiode ${from} → ${to}`);
  });

  renderVerticalCards("permitHistResult", cards);
  document.getElementById("permitHistDetails").innerHTML = "";
});

document.addEventListener("click", (e) => {
  const btn = e.target.closest(".details-btn");
  if (!btn) return;

  const targetId = btn.getAttribute("data-target");
  const permitId = btn.getAttribute("data-permit");
  const holderId = (btn.getAttribute("data-holder") || "").trim() || null;
  const date = (btn.getAttribute("data-date") || "").trim() || null;

  const rowJson = getSnapshotRowJson(permitId, holderId, date);
  if (!rowJson) {
    document.getElementById(targetId).innerHTML = "<p>Fant ingen snapshot-rad for denne kombinasjonen.</p>";
    return;
  }
  document.getElementById(targetId).innerHTML = renderKeyValueTableHtml(JSON.parse(rowJson), `Detaljer for ${permitId}`);
});

// -------------------- Filter buttons --------------------
document.getElementById("filterAllBtn").addEventListener("click", () => {
  activeFilter = null;
  setActiveFilterUi();
  clearAllSearchUi();
});

document.getElementById("filterGrunnrenteBtn").addEventListener("click", () => {
  activeFilter = "Grunnrenteskatteplikt";
  setActiveFilterUi();
  clearAllSearchUi();
});

// -------------------- Clear-on-empty + Enter-to-search --------------------
function wireInputClearAndEnter(inputId, onEnterClickId, clearFn) {
  const input = document.getElementById(inputId);
  input.addEventListener("input", (e) => {
    if (e.target.value.trim() === "") clearFn();
  });
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById(onEnterClickId).click();
  });
}

wireInputClearAndEnter("holderNowInput", "holderNowBtn", clearHolderNowUI);
wireInputClearAndEnter("holderHistInput", "holderHistBtn", clearHolderHistUI);
wireInputClearAndEnter("permitNowInput", "permitNowBtn", clearPermitNowUI);
wireInputClearAndEnter("permitHistInput", "permitHistBtn", clearPermitHistUI);

// -------------------- Start --------------------
(async function main() {
  try {
    latestMeta = await loadLatestJson();
    const sqlJs = await loadSqlJs();
    await downloadAndOpenDb(sqlJs);

    setActiveFilterUi();
  } catch (e) {
    console.error(e);
    setStatus("Feil: " + e.message);
  }
})();
