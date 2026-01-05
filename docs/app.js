let db = null;
let latestMeta = null;

// Aktivt filter: null = alle, ellers tag-navn (f.eks. "Grunnrenteskatteplikt")
let activeFilter = null;

// PermitId-lister for “Vis detaljer” i innehaver-visningene (disse følger aktivt filter)
let holderNowPermitIds = [];
let holderHistPermitIds = [];

// Data for CSV-eksport (hva som sist ble vist)
let exportState = {
  view: "holder-now",
  holderNow: { columns: [], rows: [], filename: "" },
  holderHist: { columns: [], rows: [], filename: "" },
  permitNow: { columns: [], rows: [], filename: "" },
  permitHist: { columns: [], rows: [], filename: "" }
};

// -------------------- Details table UI state --------------------
const detailsTableState = {
  holderNowDetails: null,
  holderHistDetails: null
};

function cellToText(cell) {
  if (cell == null) return "";
  // Støtter våre HTML-celler: {__html: "..."}
  if (typeof cell === "object" && cell.__html) {
    // Fjern HTML tags for sortering/filtrering
    return String(cell.__html).replace(/<[^>]*>/g, "").trim();
  }
  return String(cell).trim();
}

function uniqueValues(rows, colIdx) {
  const set = new Set();
  for (const r of rows) {
    const v = cellToText(r[colIdx]);
    if (v) set.add(v);
  }
  return Array.from(set).sort((a, b) => a.localeCompare(b, "no"));
}

function applyDetailsFilters(baseRows, columns, state) {
  let rows = baseRows.slice();

  // Fritekstfilter
  if (state.text && state.text.trim() !== "") {
    const q = state.text.trim().toLowerCase();
    rows = rows.filter(r => r.some(c => cellToText(c).toLowerCase().includes(q)));
  }

  // Dropdown-filters
  for (const [colName, selected] of Object.entries(state.filters || {})) {
    if (!selected) continue;
    const idx = columns.indexOf(colName);
    if (idx >= 0) {
      rows = rows.filter(r => cellToText(r[idx]) === selected);
    }
  }

  // Sortering
  if (state.sortCol != null) {
    const idx = state.sortCol;
    const dir = state.sortDir === "desc" ? -1 : 1;
    rows.sort((a, b) => {
      const av = cellToText(a[idx]);
      const bv = cellToText(b[idx]);
      // Numerisk dersom begge ser ut som tall
      const an = Number(av.replace(",", "."));
      const bn = Number(bv.replace(",", "."));
      const bothNumeric = !Number.isNaN(an) && !Number.isNaN(bn) && av !== "" && bv !== "";
      if (bothNumeric) return (an - bn) * dir;
      return av.localeCompare(bv, "no") * dir;
    });
  }

  return rows;
}

function renderDetailsToolbarAndTable(containerId) {
  const s = detailsTableState[containerId];
  const el = document.getElementById(containerId);
  if (!el || !s) return;

  const columns = s.columns;
  const baseRows = s.baseRows;

  // Hvilke kolonner er skjult?
  const hidden = s.hiddenCols || new Set();

  // Beregn rader etter filter/sort
  const rows = applyDetailsFilters(baseRows, columns, s);

  // Bygg toolbar (dropdowns + fritekst + kolonnevalg)
  // Vi tilbyr filtre for "type"-felt: STATUS, ART, FORMÅL, PRODUKSJONSFORM
  const filterCols = ["STATUS", "ART", "FORMÅL", "PRODUKSJONSFORM"].filter(c => columns.includes(c));

  const filterControls = filterCols.map(colName => {
    const idx = columns.indexOf(colName);
    const options = uniqueValues(baseRows, idx);
    const selected = (s.filters && s.filters[colName]) ? s.filters[colName] : "";
    const optsHtml = [`<option value="">Alle</option>`]
      .concat(options.map(v => `<option value="${escapeHtml(v)}"${v === selected ? " selected" : ""}>${escapeHtml(v)}</option>`))
      .join("");
    return `
      <label class="dt-label">${escapeHtml(colName)}
        <select class="dt-select" data-dt-filter="${escapeHtml(colName)}">
          ${optsHtml}
        </select>
      </label>
    `;
  }).join("");

  // Kolonne toggles
  const colToggles = columns.map((c) => {
    const checked = !hidden.has(c);
    return `
      <label class="dt-check">
        <input type="checkbox" data-dt-col="${escapeHtml(c)}" ${checked ? "checked" : ""}>
        ${escapeHtml(String(c).toUpperCase())}
      </label>
    `;
  }).join("");

  // Tabell-header (klikk for sortering) – respekter skjulte kolonner
  const headerHtml = columns.map((c, idx) => {
    if (hidden.has(c)) return "";
    const isSort = s.sortCol === idx;
    const arrow = isSort ? (s.sortDir === "desc" ? " ▼" : " ▲") : "";
    return `<th data-dt-sort="${idx}" class="dt-sort">${escapeHtml(String(c).toUpperCase())}${arrow}</th>`;
  }).join("");

  // Tabell-body – respekter skjulte kolonner
  const bodyHtml = rows.map(r => {
    const tds = r.map((cell, i) => {
      if (hidden.has(columns[i])) return "";
      if (cell && typeof cell === "object" && cell.__html) return `<td>${cell.__html}</td>`;
      return `<td>${cell == null ? "" : escapeHtml(String(cell))}</td>`;
    }).join("");
    return `<tr>${tds}</tr>`;
  }).join("");

  el.innerHTML = `
    <div class="details-area">
      <div class="details-toolbar">
        <div class="details-toolbar-row">
          <label class="dt-label">Søk i detaljer
            <input class="dt-input" type="search" placeholder="Filtrer i tabellen…" value="${escapeHtml(s.text || "")}">
          </label>
          ${filterControls}
          <div class="dt-meta">${rows.length} rader</div>
        </div>

        <details class="dt-columns">
          <summary>Vis/skjul kolonner</summary>
          <div class="dt-columns-grid">
            ${colToggles}
          </div>
        </details>
      </div>

      <table class="zebra">
        <thead><tr>${headerHtml}</tr></thead>
        <tbody>${bodyHtml}</tbody>
      </table>
    </div>
  `;

  // Wire events (delegert inne i container)
  const toolbarRoot = el.querySelector(".details-area");
  if (!toolbarRoot) return;

  // Fritekst
  const textInput = toolbarRoot.querySelector(".dt-input");
  if (textInput) {
    textInput.addEventListener("input", (e) => {
      s.text = e.target.value || "";
      renderDetailsToolbarAndTable(containerId);
    });
  }

  // Dropdowns
  toolbarRoot.querySelectorAll("select[data-dt-filter]").forEach(sel => {
    sel.addEventListener("change", (e) => {
      const col = e.target.getAttribute("data-dt-filter");
      s.filters = s.filters || {};
      s.filters[col] = e.target.value || "";
      renderDetailsToolbarAndTable(containerId);
    });
  });

  // Sortering ved klikk på th
  toolbarRoot.querySelectorAll("th[data-dt-sort]").forEach(th => {
    th.addEventListener("click", (e) => {
      const idx = Number(e.target.getAttribute("data-dt-sort"));
      if (s.sortCol === idx) {
        s.sortDir = (s.sortDir === "asc") ? "desc" : "asc";
      } else {
        s.sortCol = idx;
        s.sortDir = "asc";
      }
      renderDetailsToolbarAndTable(containerId);
    });
  });

  // Skjul/vis kolonner
  toolbarRoot.querySelectorAll('input[type="checkbox"][data-dt-col]').forEach(chk => {
    chk.addEventListener("change", (e) => {
      const col = e.target.getAttribute("data-dt-col");
      s.hiddenCols = s.hiddenCols || new Set();
      if (e.target.checked) s.hiddenCols.delete(col);
      else s.hiddenCols.add(col);
      renderDetailsToolbarAndTable(containerId);
    });
  });
}


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

  if (allBtn) allBtn.classList.toggle("active", activeFilter === null);
  if (grBtn) grBtn.classList.toggle("active", activeFilter === "Grunnrenteskatteplikt");

  if (label) label.textContent = activeFilter ? `Aktivt filter: ${activeFilter}` : "Aktivt filter: Alle";
}

function switchTab(tabId) {
  exportState.view = tabId;

  document.querySelectorAll(".tab").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));

  const tabBtn = document.querySelector(`.tab[data-tab="${tabId}"]`);
  const panel = document.getElementById(tabId);

  if (tabBtn) tabBtn.classList.add("active");
  if (panel) panel.classList.add("active");

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

// -------------------- Formatting helpers --------------------
function formatPermitIdForDisplay(permitId) {
  // "F A 0011" -> "F-A-0011"
  return String(permitId).trim().replace(/\s+/g, "-");
}

function normalizePermitIdForLookup(input) {
  // Tillat søk både med bindestrek og mellomrom -> DB-format med mellomrom
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

function tryExtract(obj, key) {
  const v = obj ? obj[key] : null;
  return v == null ? "" : String(v).trim();
}

function parseNumberNo(v) {
  if (v == null) return null;
  let s = String(v).trim();
  if (!s) return null;

  // Fjern mellomrom (tusenskiller), NBSP, etc.
  s = s.replace(/\s+/g, "");
  // Norsk desimal komma -> punktum
  s = s.replace(",", ".");

  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

const nfNo = new Intl.NumberFormat("nb-NO", {
  useGrouping: true,
  minimumFractionDigits: 0,
  maximumFractionDigits: 2
});

function formatNumberNo(n) {
  if (n == null || !Number.isFinite(n)) return "";
  if (Object.is(n, -0)) n = 0;
  return nfNo.format(n);
}

function normalizeUnit(unitRaw) {
  const u = (unitRaw || "").trim().toUpperCase();
  if (!u) return "";
  if (u === "TN") return "tonn";
  return u;
}

// -------------------- Permit external link helpers --------------------
function permitUrl(permitIdOrDisplay) {
  const display = formatPermitIdForDisplay(permitIdOrDisplay); // ensure F-A-0011
  return `https://sikker.fiskeridir.no/akvakulturregisteret/web/licenses/${encodeURIComponent(display)}`;
}

function permitLinkHtml(permitIdOrDisplay, label = null) {
  const display = formatPermitIdForDisplay(permitIdOrDisplay);
  const text = label || display;
  const href = permitUrl(display);
  return `<a href="${href}" target="_blank" rel="noopener noreferrer">${escapeHtml(text)}</a>`;
}

// -------------------- Rendering --------------------
function renderTable(containerId, columns, rows, opts = {}) {
  const { wrapDetailsArea = false, zebra = false } = opts;
  const el = document.getElementById(containerId);
  if (!el) return;

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
    for (const cell of r) {
      // Støtt "rå HTML" celler: { __html: "<a>...</a>" }
      if (cell && typeof cell === "object" && cell.__html) {
        html += `<td>${cell.__html}</td>`;
      } else {
        html += `<td>${cell === null ? "" : escapeHtml(String(cell))}</td>`;
      }
    }
    html += `</tr>`;
  }

  html += `</tbody></table>`;
  if (wrapDetailsArea) html += `</div>`;
  el.innerHTML = html;
}

function renderKeyValue(containerId, obj, title = null) {
  const el = document.getElementById(containerId);
  if (!el) return;

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

// Key/Value “Vis detaljer”-tabell med lenking av TILL_NR
function renderKeyValueTableHtml(obj, title = null) {
  const keys = Object.keys(obj || {});
  if (!obj || keys.length === 0) return "<p>Ingen data.</p>";

  const o = normalizeDetailsObjectForDisplay(obj);

  let html = "";
  if (title) html += `<h4>${escapeHtml(title)}</h4>`;
  html += `<div class="details-area">`;
  html += `<table class="details-kv zebra"><thead><tr><th>Felt</th><th>Verdi</th></tr></thead><tbody>`;

  for (const key of Object.keys(o).sort()) {
    const val = o[key];

    let cellHtml = (val == null) ? "" : escapeHtml(String(val));

    // Klikkbar TILL_NR
    if (key === "TILL_NR" && val != null && String(val).trim() !== "") {
      cellHtml = permitLinkHtml(val, formatPermitIdForDisplay(val));
    }

    html += `<tr><td><code>${escapeHtml(key)}</code></td><td>${cellHtml}</td></tr>`;
  }

  html += "</tbody></table>";
  html += `</div>`;
  return html;
}

function renderVerticalCards(containerId, cardsHtml) {
  const el = document.getElementById(containerId);
  if (!el) return;
  if (!cardsHtml || cardsHtml.length === 0) {
    el.innerHTML = "<p>Ingen treff.</p>";
    return;
  }
  el.innerHTML = cardsHtml.join("\n<hr />\n");
}

function cardFromSummary(summaryObj, permitIdForData, holderId, preferDate, detailsTargetId, heading) {
  const detailsBtn = `
    <button class="details-btn"
      data-target="${escapeHtml(detailsTargetId)}"
      data-permit="${escapeHtml(permitIdForData)}"
      data-holder="${escapeHtml(holderId || "")}"
      data-date="${escapeHtml(preferDate || "")}">
      Vis detaljer
    </button>
  `;
  const title = heading ? `<h4>${escapeHtml(heading)}</h4>` : "";
  const kv = renderKeyValueTableHtml(summaryObj);
  return `${title}<div class="row">${detailsBtn}</div>${kv}`;
}

// -------------------- Clear helpers --------------------
function clearHolderNowUI() {
  holderNowPermitIds = [];
  const s = document.getElementById("holderNowSummary");
  const d = document.getElementById("holderNowDetails");
  if (s) s.innerHTML = "";
  if (d) d.innerHTML = "";
  const b = document.getElementById("holderNowDetailsBtn");
  const e = document.getElementById("holderNowExportBtn");
  if (b) b.disabled = true;
  if (e) e.disabled = true;
  exportState.holderNow = { columns: [], rows: [], filename: "" };
}

function clearHolderHistUI() {
  holderHistPermitIds = [];
  const s = document.getElementById("holderHistSummary");
  const d = document.getElementById("holderHistDetails");
  if (s) s.innerHTML = "";
  if (d) d.innerHTML = "";
  const b = document.getElementById("holderHistDetailsBtn");
  const e = document.getElementById("holderHistExportBtn");
  if (b) b.disabled = true;
  if (e) e.disabled = true;
  exportState.holderHist = { columns: [], rows: [], filename: "" };
}

function clearPermitNowUI() {
  const r = document.getElementById("permitNowResult");
  const d = document.getElementById("permitNowDetails");
  if (r) r.innerHTML = "";
  if (d) d.innerHTML = "";
  const e = document.getElementById("permitNowExportBtn");
  if (e) e.disabled = true;
  exportState.permitNow = { columns: [], rows: [], filename: "" };
}

function clearPermitHistUI() {
  const r = document.getElementById("permitHistResult");
  const d = document.getElementById("permitHistDetails");
  if (r) r.innerHTML = "";
  if (d) d.innerHTML = "";
  const e = document.getElementById("permitHistExportBtn");
  if (e) e.disabled = true;
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
  setStatus(`Laster datagrunnlag (snapshot ${latestMeta.snapshot_date}) …`);

  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`Klarte ikke å laste database (HTTP ${r.status})`);

  const gzBuf = new Uint8Array(await r.arrayBuffer());
  const rawBuf = window.pako.ungzip(gzBuf);
  db = new sqlJs.Database(rawBuf);

  setStatus(`Datagrunnlag: Snapshot ${latestMeta.snapshot_date}`);
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

// -------------------- Filter helpers --------------------
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

// -------------------- URL helpers --------------------
function getCurrentViewId() {
  const activeTab = document.querySelector(".tab.active");
  return activeTab ? activeTab.dataset.tab : "holder-now";
}

function getCurrentIdForView(viewId) {
  if (viewId === "holder-now") return (document.getElementById("holderNowInput")?.value || "").trim();
  if (viewId === "holder-history") return (document.getElementById("holderHistInput")?.value || "").trim();
  if (viewId === "permit-now") return (document.getElementById("permitNowInput")?.value || "").trim();
  if (viewId === "permit-history") return (document.getElementById("permitHistInput")?.value || "").trim();
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

  activeFilter = (filter === "grunnrente") ? "Grunnrenteskatteplikt" : null;
  setActiveFilterUi();

  const validViews = new Set(["holder-now", "holder-history", "permit-now", "permit-history"]);
  const finalView = validViews.has(view) ? view : "holder-now";
  switchTab(finalView);

  if (!id) {
    updateUrlFromUi();
    return;
  }

  if (finalView === "holder-now") document.getElementById("holderNowInput").value = id;
  if (finalView === "holder-history") document.getElementById("holderHistInput").value = id;
  if (finalView === "permit-now") document.getElementById("permitNowInput").value = id;
  if (finalView === "permit-history") document.getElementById("permitHistInput").value = id;

  if (finalView === "holder-now") document.getElementById("holderNowBtn").click();
  if (finalView === "holder-history") document.getElementById("holderHistBtn").click();
  if (finalView === "permit-now") document.getElementById("permitNowBtn").click();
  if (finalView === "permit-history") document.getElementById("permitHistBtn").click();
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

// NY: hent ALLE snapshot-rader (alle arter/lokasjoner) for permit+holder i siste snapshot
function getLatestSnapshotRowJsonsForPermitHolder(permitId, holderId) {
  const q = `
    SELECT row_json
    FROM permit_snapshot
    WHERE snapshot_date = $d AND permit_id = $p AND holder_id = $h
  `;
  const rows = runQuery(q, { $d: latestMeta.snapshot_date, $p: permitId, $h: holderId });
  return rows.map(r => r[0]).filter(Boolean);
}

// NY: hent ALLE snapshot-rader for permit (+holder) for foretrukket dato, ellers nyeste dato
function getSnapshotRowJsons(permitId, holderId = null, preferDate = null) {
  // 1) Hvis vi har en preferert dato, prøv den først
  if (preferDate) {
    const q1 = holderId
      ? `SELECT row_json
         FROM permit_snapshot
         WHERE snapshot_date=$d AND permit_id=$p AND holder_id=$h`
      : `SELECT row_json
         FROM permit_snapshot
         WHERE snapshot_date=$d AND permit_id=$p`;

    const r1 = runQuery(q1, holderId
      ? { $d: preferDate, $p: permitId, $h: holderId }
      : { $d: preferDate, $p: permitId }
    );

    if (r1.length) return r1.map(r => r[0]).filter(Boolean);
  }

  // 2) Finn nyeste snapshot_date for permit (+holder hvis valgt)
  const q2 = holderId
    ? `SELECT snapshot_date
       FROM permit_snapshot
       WHERE permit_id=$p AND holder_id=$h
       ORDER BY snapshot_date DESC
       LIMIT 1`
    : `SELECT snapshot_date
       FROM permit_snapshot
       WHERE permit_id=$p
       ORDER BY snapshot_date DESC
       LIMIT 1`;

  const d = runQuery(q2, holderId ? { $p: permitId, $h: holderId } : { $p: permitId });
  const lastDate = d.length ? d[0][0] : null;
  if (!lastDate) return [];

  // 3) Hent alle rader for den datoen
  const q3 = holderId
    ? `SELECT row_json
       FROM permit_snapshot
       WHERE snapshot_date=$d AND permit_id=$p AND holder_id=$h`
    : `SELECT row_json
       FROM permit_snapshot
       WHERE snapshot_date=$d AND permit_id=$p`;

  const r3 = runQuery(q3, holderId
    ? { $d: lastDate, $p: permitId, $h: holderId }
    : { $d: lastDate, $p: permitId }
  );

  return r3.map(r => r[0]).filter(Boolean);
}

// NY: bygg ART-liste + hovedkapasitet (maksverdi av TILL_KAP)
function summarizePermitSnapshotRowsMainCapacity(rowJsons) {
  const arts = new Set();
  let unit = "";
  let mainCap = null;

  for (const rj of rowJsons) {
    const obj = JSON.parse(rj);

    const art = tryExtract(obj, "ART");
    if (art) arts.add(art);

    if (!unit) unit = normalizeUnit(tryExtract(obj, "TILL_ENHET"));

    const kap = parseNumberNo(tryExtract(obj, "TILL_KAP"));
    if (kap != null) mainCap = (mainCap == null) ? kap : Math.max(mainCap, kap);
  }

  const artStr = [...arts].join(", ");

  // Vis tomt hvis mangler/0
  const capStr = (mainCap == null || mainCap <= 0)
    ? ""
    : `${formatNumberNo(mainCap)} ${unit}`.trim();

  return { artStr, capStr };
}

function getSnapshotRowJson(permitId, holderId = null, preferDate = null) {
  // (Beholdes for bakoverkompat / hvis du vil slå opp én rad et sted)
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

// -------------------- Status for holder history (aktiv/avsluttet) --------------------
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

// -------------------- Holder details table (horisontal + zebra) --------------------
function renderHolderDetailsTable(containerId, holderId, permitIds) {
  const el = document.getElementById(containerId);
  if (!el) return { columns: [], rows: [] };

  if (!permitIds || permitIds.length === 0) {
    el.innerHTML = "<p>Ingen tillatelser å vise.</p>";
    return { columns: [], rows: [] };
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
    // NY: hent alle snapshot-rader i stedet for LIMIT 1
    const rowJsons = getLatestSnapshotRowJsonsForPermitHolder(permitId, holderId);

    if (!rowJsons.length) {
      rows.push([
        getPermitStatusForHolder(holderId, permitId),
        { __html: permitLinkHtml(permitId) },
        "",
        "",
        "",
        "",
        "",
        ""
      ]);
      continue;
    }

    const firstObj = JSON.parse(rowJsons[0]); // stabile felt
    const { artStr, capStr } = summarizePermitSnapshotRowsMainCapacity(rowJsons);

    rows.push([
      getPermitStatusForHolder(holderId, permitId),
      { __html: permitLinkHtml(permitId) }, // 👈 klikkbar lenke
      artStr,
      tryExtract(firstObj, "FORMÅL"),
      tryExtract(firstObj, "PRODUKSJONSFORM"),
      tryExtract(firstObj, "TILDELINGSTIDSPUNKT"),
      capStr,
      tryExtract(firstObj, "PROD_OMR")
    ]);
  }

  rows.sort((a, b) => {
    const ra = String(a[0]).startsWith("Aktiv") ? 0 : 1;
    const rb = String(b[0]).startsWith("Aktiv") ? 0 : 1;
    if (ra !== rb) return ra - rb;
    // sammenlign på tekst (tillatelsesnummer)
    const aTxt = (a[1] && typeof a[1] === "object" && a[1].__html) ? a[1].__html : String(a[1]);
    const bTxt = (b[1] && typeof b[1] === "object" && b[1].__html) ? b[1].__html : String(b[1]);
    return aTxt.localeCompare(bTxt, "no");
  });

  // Lagre state for sortering/filtrering/kolonner og render med toolbar
  detailsTableState[containerId] = {
    columns,
    baseRows: rows,          // original
    text: "",
    filters: {},             // STATUS/ART/FORMÅL/PRODUKSJONSFORM
    sortCol: null,
    sortDir: "asc",
    hiddenCols: new Set()    // tom = vis alt
  };

  renderDetailsToolbarAndTable(containerId);
  return { columns, rows };
}

// -------------------- Permit details table (for "Vis detaljer" in permit views) --------------------
function renderPermitSnapshotDetails(containerId, permitId, holderId = null, preferDate = null) {
  const el = document.getElementById(containerId);
  if (!el) return { columns: [], rows: [] };

  const rowJsons = getSnapshotRowJsons(permitId, holderId, preferDate);
  if (!rowJsons.length) {
    el.innerHTML = "<p>Fant ingen snapshot-rader for denne kombinasjonen.</p>";
    return { columns: [], rows: [] };
  }

  // Aggregert toppinfo (alle arter + hovedkapasitet)
  const { artStr, capStr } = summarizePermitSnapshotRowsMainCapacity(rowJsons);
  const permitDisplay = formatPermitIdForDisplay(permitId);

  // Bygg en detalj-tabell med én linje per snapshot-rad (typisk per art)
  const objs = rowJsons.map(rj => JSON.parse(rj));

  // Velg “nyttige” kolonner som ofte finnes i row_json
  const preferredCols = [
    "ART",
    "TILL_KAP",
    "TILL_ENHET",
    "FORMÅL",
    "PRODUKSJONSFORM",
    "TILDELINGSTIDSPUNKT",
    "PROD_OMR",
    "VANNMILJØ",
    "KOMMUNE",
    "LOK_NR",
    "LOK_NAVN"
  ];

  const present = new Set();
  for (const o of objs) {
    for (const k of Object.keys(o || {})) present.add(k);
  }

  const columns = preferredCols.filter(c => present.has(c));
  // Fallback hvis uventet: vis i det minste ART hvis den finnes
  const safeColumns = columns.length ? columns : (present.has("ART") ? ["ART"] : ["(rad)"]);

  const rows = objs.map(o => safeColumns.map(c => {
    if (c === "(rad)") return JSON.stringify(o);
    if (c === "TILL_ENHET") return normalizeUnit(tryExtract(o, c));
    if (c === "TILL_KAP") {
      const n = parseNumberNo(tryExtract(o, c));
      return (n == null) ? "" : formatNumberNo(n);
    }
    return tryExtract(o, c);
  }));

  // Vi vil vise et aggregert “header”-område + tabell med toolbar.
  // renderDetailsToolbarAndTable renderer hele containeren, så vi lager en wrapper:
  el.innerHTML = `
    <div class="details-area">
      <h4>Detaljer for ${escapeHtml(permitDisplay)}</h4>
      <div class="details-kv">
        <div><b>Tillatelse:</b> ${permitLinkHtml(permitId, permitDisplay)}</div>
        ${holderId ? `<div><b>Innehaver:</b> <code>${escapeHtml(holderId)}</code></div>` : ""}
        ${preferDate ? `<div><b>Dato:</b> <code>${escapeHtml(preferDate)}</code></div>` : ""}
        <div><b>Arter:</b> ${escapeHtml(artStr || "")}</div>
        <div><b>Hovedkapasitet:</b> ${escapeHtml(capStr || "")}</div>
      </div>
      <div id="${escapeHtml(containerId)}__table"></div>
    </div>
  `;

  // Render selve tabellen inn i under-container, med samme toolbar-funksjonalitet
  const tableId = `${containerId}__table`;

  detailsTableState[tableId] = {
    columns: safeColumns,
    baseRows: rows,
    text: "",
    filters: {},
    sortCol: null,
    sortDir: "asc",
    hiddenCols: new Set()
  };

  // Vi må sikre at under-container finnes
  const tableEl = document.getElementById(tableId);
  if (tableEl) {
    // midlertidig: gi under-containeren et "ekte" element å jobbe med
    // ved å sette et ID som renderDetailsToolbarAndTable kan finne.
    renderDetailsToolbarAndTable(tableId);
  }

  return { columns: safeColumns, rows };
}

// -------------------- SØK: Innehaver – gjeldende --------------------
document.getElementById("holderNowBtn").addEventListener("click", () => {
  const holderId = (document.getElementById("holderNowInput").value || "").trim();
  if (!holderId) { clearHolderNowUI(); updateUrlFromUi(); return; }

  const qAll = `
    SELECT DISTINCT permit_id
    FROM ownership_intervals
    WHERE holder_id = $h AND valid_to IS NULL
    ORDER BY permit_id
  `;
  const allRows = runQuery(qAll, { $h: holderId });
  const allPermitIds = allRows.map(r => r[0]);

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
    const s = document.getElementById("holderNowSummary");
    if (s) s.innerHTML = "<p>Ingen aktive tillatelser funnet for denne innehaveren.</p>";
    const d = document.getElementById("holderNowDetails");
    if (d) d.innerHTML = "";
    document.getElementById("holderNowDetailsBtn").disabled = true;
    document.getElementById("holderNowExportBtn").disabled = true;
    exportState.holderNow = { columns: [], rows: [], filename: "" };
    updateUrlFromUi();
    return;
  }

  let navn = "";
  const anyRowJson = getLatestSnapshotRowJsonForHolder(holderId);
  if (anyRowJson) navn = tryExtract(JSON.parse(anyRowJson), "NAVN");

  const totalCount = allPermitIds.length;
  const grunnrenteCount = countGrunnrenteForPermits(allPermitIds);

  renderKeyValue("holderNowSummary", {
    "Organisasjonsnummer / fødselsnummer": holderId,
    "Selskapsnavn": navn || "(mangler)",
    "Antall tillatelser (totalt)": totalCount,
    "Antall tillatelser (Grunnrenteskatteplikt)": grunnrenteCount
  });

  document.getElementById("holderNowDetails").innerHTML = "";
  document.getElementById("holderNowDetailsBtn").disabled = (activeFilter && holderNowPermitIds.length === 0);

  exportState.holderNow = {
    columns: [],
    rows: [],
    filename: `innehaver_gjeldende_${holderId}_${latestMeta.snapshot_date}.csv`
  };
  document.getElementById("holderNowExportBtn").disabled = (activeFilter && holderNowPermitIds.length === 0);

  updateUrlFromUi();
});

document.getElementById("holderNowDetailsBtn").addEventListener("click", () => {
  const holderId = (document.getElementById("holderNowInput").value || "").trim();
  if (!holderId) { clearHolderNowUI(); updateUrlFromUi(); return; }

  const { columns, rows } = renderHolderDetailsTable("holderNowDetails", holderId, holderNowPermitIds);

  // CSV: konverter celler til tekst (inkl. HTML-celler som TILL_NR-lenke)
  exportState.holderNow.columns = columns;
  exportState.holderNow.rows = rows.map(r => r.map(cellToText));
  document.getElementById("holderNowExportBtn").disabled = rows.length === 0;

  updateUrlFromUi();
});

// -------------------- SØK: Innehaver – historikk --------------------
document.getElementById("holderHistBtn").addEventListener("click", () => {
  const holderId = (document.getElementById("holderHistInput").value || "").trim();
  if (!holderId) { clearHolderHistUI(); updateUrlFromUi(); return; }

  const qAll = `
    SELECT DISTINCT permit_id
    FROM ownership_intervals
    WHERE holder_id = $h
    ORDER BY permit_id
  `;
  const allRows = runQuery(qAll, { $h: holderId });
  const allPermitIds = allRows.map(r => r[0]);

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
    const s = document.getElementById("holderHistSummary");
    if (s) s.innerHTML = "<p>Ingen historikk funnet for denne innehaveren.</p>";
    const d = document.getElementById("holderHistDetails");
    if (d) d.innerHTML = "";
    document.getElementById("holderHistDetailsBtn").disabled = true;
    document.getElementById("holderHistExportBtn").disabled = true;
    exportState.holderHist = { columns: [], rows: [], filename: "" };
    updateUrlFromUi();
    return;
  }

  let navn = "";
  const anyRowJson = getLatestSnapshotRowJsonForHolder(holderId);
  if (anyRowJson) navn = tryExtract(JSON.parse(anyRowJson), "NAVN");

  const totalCount = allPermitIds.length;
  const grunnrenteCount = countGrunnrenteForPermits(allPermitIds);

  renderKeyValue("holderHistSummary", {
    "Organisasjonsnummer / fødselsnummer": holderId,
    "Selskapsnavn": navn || "(mangler)",
    "Antall tillatelser (totalt)": totalCount,
    "Antall tillatelser (Grunnrenteskatteplikt)": grunnrenteCount
  });

  document.getElementById("holderHistDetails").innerHTML = "";
  document.getElementById("holderHistDetailsBtn").disabled = (activeFilter && holderHistPermitIds.length === 0);

  exportState.holderHist = {
    columns: [],
    rows: [],
    filename: `innehaver_historikk_${holderId}_${latestMeta.snapshot_date}.csv`
  };
  document.getElementById("holderHistExportBtn").disabled = (activeFilter && holderHistPermitIds.length === 0);

  updateUrlFromUi();
});

document.getElementById("holderHistDetailsBtn").addEventListener("click", () => {
  const holderId = (document.getElementById("holderHistInput").value || "").trim();
  if (!holderId) { clearHolderHistUI(); updateUrlFromUi(); return; }

  const { columns, rows } = renderHolderDetailsTable("holderHistDetails", holderId, holderHistPermitIds);

  // CSV: konverter celler til tekst (inkl. HTML-celler som TILL_NR-lenke)
  exportState.holderHist.columns = columns;
  exportState.holderHist.rows = rows.map(r => r.map(cellToText));
  document.getElementById("holderHistExportBtn").disabled = rows.length === 0;

  updateUrlFromUi();
});

// -------------------- SØK: Tillatelse – eier (nå) --------------------
document.getElementById("permitNowBtn").addEventListener("click", () => {
  const raw = (document.getElementById("permitNowInput").value || "").trim();
  if (!raw) { clearPermitNowUI(); updateUrlFromUi(); return; }

  const permitId = normalizePermitIdForLookup(raw);
  const permitDisplay = formatPermitIdForDisplay(permitId);

  if (activeFilter && !permitIsInActiveFilter(permitId)) {
    document.getElementById("permitNowResult").innerHTML =
      `<p>Tillatelsen <code>${escapeHtml(raw)}</code> er ikke innen filteret “${escapeHtml(activeFilter)}”.</p>`;
    document.getElementById("permitNowDetails").innerHTML = "";
    document.getElementById("permitNowExportBtn").disabled = true;
    exportState.permitNow = { columns: [], rows: [], filename: "" };
    updateUrlFromUi();
    return;
  }

  // NY: hent alle snapshot-rader (alle arter) for siste snapshot
  const rowJsons = getSnapshotRowJsons(permitId, null, latestMeta.snapshot_date);
  if (!rowJsons.length) {
    document.getElementById("permitNowResult").innerHTML =
      `<p>Fant ingen tillatelse med dette nummeret: <code>${escapeHtml(raw)}</code></p>`;
    document.getElementById("permitNowDetails").innerHTML = "";
    document.getElementById("permitNowExportBtn").disabled = true;
    exportState.permitNow = { columns: [], rows: [], filename: "" };
    updateUrlFromUi();
    return;
  }

  const firstObj = JSON.parse(rowJsons[0]);
  const { artStr, capStr } = summarizePermitSnapshotRowsMainCapacity(rowJsons);

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
    "TILL_NR": permitDisplay,
    "NAVN": tryExtract(firstObj, "NAVN") || "(mangler)",
    "ART": artStr,
    "Hovedkapasitet": capStr,
    "Snapshot": latestMeta.snapshot_date,
    "Eier": eier ? `${eier} (fra ${eierFra})` : "(ikke funnet)"
  };

  renderVerticalCards("permitNowResult", [
    cardFromSummary(summary, permitId, eier || "", latestMeta.snapshot_date, "permitNowDetails", `Tillatelse ${permitDisplay}`)
  ]);
  document.getElementById("permitNowDetails").innerHTML = "";

  exportState.permitNow = {
    filename: `tillatelse_eier_${permitDisplay}_${latestMeta.snapshot_date}.csv`,
    columns: ["TILL_NR", "ART", "HOVEDKAPASITET", "HOLDER_ID", "VALID_FROM", "SNAPSHOT_DATE"],
    rows: [[permitDisplay, artStr || "", capStr || "", eier || "", eierFra || "", latestMeta.snapshot_date]]
  };
  document.getElementById("permitNowExportBtn").disabled = false;

  updateUrlFromUi();
});

// -------------------- SØK: Tillatelse – historikk --------------------
document.getElementById("permitHistBtn").addEventListener("click", () => {
  const raw = (document.getElementById("permitHistInput").value || "").trim();
  if (!raw) { clearPermitHistUI(); updateUrlFromUi(); return; }

  const permitId = normalizePermitIdForLookup(raw);
  const permitDisplay = formatPermitIdForDisplay(permitId);

  if (activeFilter && !permitIsInActiveFilter(permitId)) {
    document.getElementById("permitHistResult").innerHTML =
      `<p>Tillatelsen <code>${escapeHtml(raw)}</code> er ikke innen filteret “${escapeHtml(activeFilter)}”.</p>`;
    document.getElementById("permitHistDetails").innerHTML = "";
    document.getElementById("permitHistExportBtn").disabled = true;
    exportState.permitHist = { columns: [], rows: [], filename: "" };
    updateUrlFromUi();
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
    document.getElementById("permitHistResult").innerHTML =
      `<p>Ingen historikk funnet for denne tillatelsen: <code>${escapeHtml(raw)}</code></p>`;
    document.getElementById("permitHistDetails").innerHTML = "";
    document.getElementById("permitHistExportBtn").disabled = true;
    exportState.permitHist = { columns: [], rows: [], filename: "" };
    updateUrlFromUi();
    return;
  }

  const cards = rows.map(([holderId, from, to]) => {
    // NY: hent ALLE rader på "from"-dato for å få alle arter + hovedkapasitet for perioden
    const rowJsons = getSnapshotRowJsons(permitId, holderId, from);

    let navn = "";
    let artStr = "";
    let capStr = "";

    if (rowJsons.length) {
      const firstObj = JSON.parse(rowJsons[0]);
      navn = tryExtract(firstObj, "NAVN");
      const s = summarizePermitSnapshotRowsMainCapacity(rowJsons);
      artStr = s.artStr;
      capStr = s.capStr;
    }

    const summary = {
      "TILL_NR": permitDisplay,
      "ORG.NR/PERS.NR": holderId,
      "NAVN": navn || "(mangler)",
      "ART": artStr,
      "Hovedkapasitet": capStr,
      "Fra": from,
      "Til": to
    };
    return cardFromSummary(summary, permitId, holderId, from, "permitHistDetails", `Eierperiode ${from} → ${to}`);
  });

  renderVerticalCards("permitHistResult", cards);
  document.getElementById("permitHistDetails").innerHTML = "";

  exportState.permitHist = {
    filename: `tillatelse_historikk_${permitDisplay}_${latestMeta.snapshot_date}.csv`,
    columns: ["TILL_NR", "HOLDER_ID", "VALID_FROM", "VALID_TO"],
    rows: rows.map(r => [permitDisplay, r[0] || "", r[1] || "", r[2] || ""])
  };
  document.getElementById("permitHistExportBtn").disabled = false;

  updateUrlFromUi();
});

// -------------------- “Vis detaljer” click (for cards) --------------------
document.addEventListener("click", (e) => {
  const btn = e.target.closest(".details-btn");
  if (!btn) return;

  const targetId = btn.getAttribute("data-target");
  const permitId = btn.getAttribute("data-permit"); // DB-format (mellomrom)
  const holderId = (btn.getAttribute("data-holder") || "").trim() || null;
  const date = (btn.getAttribute("data-date") || "").trim() || null;

  // NY: vis detaljer som en tabell med alle snapshot-rader (alle arter),
  // med aggregert “Arter” + “Hovedkapasitet” øverst.
  renderPermitSnapshotDetails(targetId, permitId, holderId, date);
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

// -------------------- Clear-on-empty + Enter-to-search --------------------
function wireInputClearAndEnter(inputId, onEnterClickId, clearFn) {
  const input = document.getElementById(inputId);
  input.addEventListener("input", (e) => {
    if (e.target.value.trim() === "") { clearFn(); updateUrlFromUi(); }
    else updateUrlFromUi();
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
    applyUrlParamsIfAny();
  } catch (e) {
    console.error(e);
    setStatus("Feil: " + e.message);
  }
})();
