/* global initSqlJs */

// =========================
// app.js (full replacement, schema-safe, robust permit search)
// Fokus på robust historisk tillatelse-søk:
// - Normaliser permit_key (case + whitespace) i både JS og SQL
// - Unngå “forrige resultat” ved å aldri vise result-layout før vi har gyldige data
// - Fjern dobbeltdefinerte helpers + fiks null-bug i historisk card
// - Rydd visning atomisk ved tom input
// - Bevarer: schema-safe art, datoformat, tidsbegrenset-logikk, UI/empty state
// =========================

let SQL = null;
let db = null;

const DB_URL = "data/aqua.sqlite";

// Schema flags (settes etter DB lastes)
const schema = {
  permit_current_has_art: false,
  permit_snapshot_has_art: false,
};

// --- helpers ---
function $(id) { return document.getElementById(id); }

function safeEl(id) {
  const el = $(id);
  if (!el) throw new Error(`Mangler element i HTML: #${id}`);
  return el;
}

function setStatus(text, kind) {
  const el = safeEl("dbStatus");
  el.textContent = text;

  // sørg for at det alltid ser ut som en pill
  el.classList.add("pill");

  // statusfarger (valgfritt)
  el.classList.remove("ok", "warn", "bad");
  if (kind) el.classList.add(kind);
}

function setMeta(text) {
  safeEl("dbMeta").textContent = text || "";
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function toHashNow() { location.hash = "#/now"; }

function normalizePermitKey(raw) {
  return (raw ?? "")
    .toString()
    .trim()
    .replace(/\s+/g, "") // fjern også whitespace inni
    .toUpperCase();
}

function toHashPermit(key) {
  const norm = normalizePermitKey(key);
  location.hash = `#/permit/${encodeURIComponent(norm)}`;
}

function toHashOwner(identity) {
  location.hash = `#/owner/${encodeURIComponent(String(identity ?? "").trim())}`;
}

function setActiveTab(tabId) {
  for (const id of ["tab-now", "tab-permit", "tab-owner"]) {
    const el = $(id);
    if (!el) continue;
    el.classList.toggle("active", id === tabId);
  }
}

function showView(viewId) {
  for (const id of ["view-now", "view-permit", "view-owner"]) {
    const el = $(id);
    if (!el) continue;
    el.style.display = (id === viewId) ? "block" : "none";
  }
}

function execAll(sql, params = []) {
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

function one(sql, params = []) {
  const rows = execAll(sql, params);
  return rows.length ? rows[0] : null;
}

function iso10(s) {
  if (!s) return null;
  const t = String(s).trim();
  if (!t) return null;
  return t.slice(0, 10);
}

function displayDate(s) {
  if (s == null) return "";
  return iso10(s) || String(s);
}

function formatNorwegianDate(isoDate) {
  // Forventer f.eks. "2026-01-13"
  const d = new Date(isoDate + "T00:00:00");
  return new Intl.DateTimeFormat("nb-NO", {
    day: "numeric",
    month: "long",
    year: "numeric"
  }).format(d);
}

function hasColumn(table, col) {
  const rows = execAll(`PRAGMA table_info(${table});`);
  return rows.some(r => String(r.name) === col);
}

// --- validation ---
function isNineDigits(s) {
  return /^[0-9]{9}$/.test(String(s || "").trim().replace(/\s+/g, ""));
}

// --- PERMIT empty state helpers ---
function setPermitEmptyStateVisible(visible) {
  const el = $("permitEmptyState");
  if (el) el.classList.toggle("hidden", !visible);
}

function setPermitEmptyStateContent({ icon, title, text }) {
  const root = $("permitEmptyState");
  if (!root) return;

  const iconEl = root.querySelector(".empty-icon");
  const titleEl = root.querySelector(".empty-title");
  const textEl  = root.querySelector(".empty-text");

  if (iconEl && icon != null) iconEl.textContent = icon;
  if (titleEl && title != null) titleEl.textContent = title;
  if (textEl  && text  != null) textEl.textContent  = text;
}

function setPermitResultsVisible(visible) {
  const split = safeEl("view-permit").querySelector(".split");
  if (split) split.classList.toggle("hidden", !visible);
}

// --- clear helpers ---
function clearPermitView() {
  // Atomisk: skjul result og vis empty-state, rydd alt innhold
  setPermitResultsVisible(false);
  setPermitEmptyStateVisible(true);

  // Noen prosjekter har både "permitEmpty" (legacy) og permitEmptyState (ny)
  const empty = $("permitEmpty");
  if (empty) empty.textContent = "";

  const card = $("permitCard");
  if (card) {
    card.classList.add("hidden");
    // valgfritt: card.innerHTML = "";
  }

  const tbody = safeEl("permitHistoryTable").querySelector("tbody");
  if (!tbody) throw new Error("Mangler <tbody> i #permitHistoryTable");
  tbody.innerHTML = "";

  const reasonTh = $("permitReasonTh");
  if (reasonTh) reasonTh.style.display = ""; // default reset
}

function clearOwnerView() {
  safeEl("ownerEmpty").textContent = "";
  safeEl("ownerCard").classList.add("hidden");

  const a = safeEl("ownerActiveTable").querySelector("tbody");
  const h = safeEl("ownerHistoryTable").querySelector("tbody");
  if (!a) throw new Error("Mangler <tbody> i #ownerActiveTable");
  if (!h) throw new Error("Mangler <tbody> i #ownerHistoryTable");
  a.innerHTML = "";
  h.innerHTML = "";
}

// --- sort state (NOW) ---
const sortState = {
  now: { key: "permit_key", dir: 1 } // dir: 1 asc, -1 desc
};

// --- load db ---
async function loadDatabase() {
  setStatus("Laster database…");
  setMeta("");

  if (!SQL) {
    SQL = await initSqlJs({
      locateFile: (f) => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${f}`,
    });
  }

  // Cache-bust
  const res = await fetch(`${DB_URL}?v=${Date.now()}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`Kunne ikke hente ${DB_URL} (HTTP ${res.status})`);
  const buf = await res.arrayBuffer();

  if (db) db.close();
  db = new SQL.Database(new Uint8Array(buf));

  // Detect schema (for å unngå "no such column: art" på eldre DB)
  schema.permit_current_has_art = hasColumn("permit_current", "art");
  schema.permit_snapshot_has_art = hasColumn("permit_snapshot", "art");

  const snap = one(`SELECT MAX(snapshot_date) AS max_date, COUNT(*) AS n FROM snapshots;`);
  setStatus("DB lastet", "ok");

  const snapIso = snap?.max_date ? String(snap.max_date).slice(0, 10) : "";
  const snapNo = snapIso ? formatNorwegianDate(snapIso) : "";

  setMeta(
    snapNo
      ? `Visningen senest oppdatert mot Akvakulturregisteret: ${snapNo}`
      : "Visningen senest oppdatert mot Akvakulturregisteret: (ukjent dato)"
  );

  renderRoute();
}

// --- NOW view ---
function renderNow() {
  setActiveTab("tab-now");
  showView("view-now");

  const q = safeEl("nowSearch").value.trim().toLowerCase();
  const only = safeEl("onlyGrunnrente").checked;

  const rows = execAll(`
    SELECT
      permit_key AS permit_key,
      owner_name AS owner_name,
      owner_identity AS owner_identity,
      owner_orgnr AS owner_orgnr,
      grunnrente_pliktig AS grunnrente_pliktig
    FROM permit_current
    ${only ? "WHERE grunnrente_pliktig = 1" : ""}
  `);

  const filtered = q
    ? rows.filter(r =>
        String(r.permit_key ?? "").toLowerCase().includes(q) ||
        String(r.owner_name ?? "").toLowerCase().includes(q) ||
        String(r.owner_identity ?? "").toLowerCase().includes(q) ||
        String(r.owner_orgnr ?? "").toLowerCase().includes(q)
      )
    : rows;

  // sort (client-side)
  const { key, dir } = sortState.now;
  filtered.sort((a, b) => {
    const av = String(a[key] ?? "");
    const bv = String(b[key] ?? "");
    return av.localeCompare(bv, "nb", { numeric: true, sensitivity: "base" }) * dir;
  });

  safeEl("nowSummary").textContent =
    `Antall tillatelser i visningen: ${filtered.length}` + (only ? " (grunnrentepliktig)" : "");

  const tbody = safeEl("nowTable").querySelector("tbody");
  if (!tbody) throw new Error("Mangler <tbody> i #nowTable");
  tbody.innerHTML = "";

  const MAX = 2500;
  const displayRows = filtered.slice(0, MAX);

  for (const r of displayRows) {
    const tr = document.createElement("tr");

    const orgnrOrIdent = (r.owner_orgnr && String(r.owner_orgnr).trim())
      ? String(r.owner_orgnr).trim()
      : String(r.owner_identity ?? "");

    const ownerIdent = (r.owner_identity && String(r.owner_identity).trim())
      ? String(r.owner_identity).trim()
      : "";

    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(String(r.permit_key ?? ""))}">${escapeHtml(r.permit_key)}</a></td>
      <td>${escapeHtml(r.owner_name)}</td>
      <td>${
        ownerIdent
          ? `<a class="link" href="#/owner/${encodeURIComponent(ownerIdent)}">${escapeHtml(orgnrOrIdent)}</a>`
          : `${escapeHtml(orgnrOrIdent)}`
      }</td>
    `;

    tbody.appendChild(tr);
  }

  if (filtered.length > MAX) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td colspan="3" class="muted">
        Viser kun de første ${MAX} radene. Begrens søket for å se resten.
      </td>
    `;
    tbody.appendChild(tr);
  }
}

// --- PERMIT view ---
function renderPermit(permitKey) {
  setActiveTab("tab-permit");
  showView("view-permit");

  // Default empty state (før vi vet noe)
  setPermitEmptyStateContent({
    icon: "🔍",
    title: "Søk etter tillatelse",
    text: "Skriv et tillatelsesnummer i feltet over for å se detaljer og historikk."
  });

  // Rydd alltid først (hindrer “hengende” forrige resultat hvis vi feiler underveis)
  clearPermitView();

  // sync input-felt med route
  if (permitKey != null) safeEl("permitInput").value = String(permitKey);

  const inputEl = safeEl("permitInput");
  const raw = String(permitKey || "");

  // Tom route/input
  const norm = normalizePermitKey(raw);
  if (!norm) {
    // allerede ryddet og vist empty state
    return;
  }

  // Hvis noen skriver orgnr i permit-feltet
  if (isNineDigits(raw)) {
    const msg = "Dette ser ut som et org.nr. (9 siffer). Bruk fanen Innehaver for org.nr., eller skriv et tillatelsesnr. (f.eks. H-F-0910).";
    const legacy = $("permitEmpty");
    if (legacy) legacy.textContent = msg;

    setPermitEmptyStateContent({
      icon: "ℹ️",
      title: "Dette ser ut som org.nr.",
      text: msg
    });
    setPermitEmptyStateVisible(true);
    return;
  }

  permitKey = norm;
  inputEl.value = permitKey;

  const permitUrl =
    `https://sikker.fiskeridir.no/akvakulturregisteret/web/licenses/${encodeURIComponent(permitKey)}`;

  // --- Queries (normalisert match i SQL) ---
  const now = one(`
    SELECT
      permit_key AS permit_key,
      owner_orgnr AS owner_orgnr,
      owner_name AS owner_name,
      owner_identity AS owner_identity,
      snapshot_date AS snapshot_date,
      grunnrente_pliktig AS grunnrente_pliktig,
      ${schema.permit_current_has_art ? "art AS art" : "NULL AS art"},
      row_json AS row_json
    FROM permit_current
    WHERE UPPER(REPLACE(TRIM(permit_key), ' ', '')) = ?;
  `, [permitKey]);

  const hist = execAll(`
    SELECT
      valid_from AS valid_from,
      valid_to   AS valid_to,
      COALESCE(NULLIF(valid_to,''), 'Aktiv') AS valid_to_label,
      owner_name AS owner_name,
      owner_orgnr AS owner_orgnr,
      owner_identity AS owner_identity,
      tidsbegrenset AS tidsbegrenset
    FROM ownership_history
    WHERE UPPER(REPLACE(TRIM(permit_key), ' ', '')) = ?
    ORDER BY date(valid_from), id;
  `, [permitKey]);

  // Ingen treff
  if (!now && hist.length === 0) {
    setPermitEmptyStateContent({
      icon: "⚠️",
      title: "Ingen tillatelser funnet",
      text: `Fant ingen tillatelser med nummeret ${permitKey}.`
    });
    setPermitEmptyStateVisible(true);
    return;
  }

  // Vi har data -> vis result-layout først nå
  setPermitResultsVisible(true);
  setPermitEmptyStateVisible(false);

  // Skal vi vise Årsak-kolonnen i permit-historikk?
  let showReasonColumn = false;
  for (let i = 0; i < hist.length; i++) {
    const r = hist[i];
    const next = hist[i + 1] || null;

    const validTo = iso10(r.valid_to);
    const tb = iso10(r.tidsbegrenset);

    let reason = "";
    if (!validTo) {
      reason = "";
    } else if (tb && tb === validTo) {
      reason = `Utløpt (tidsbegrenset ${tb})`;
    } else if (next) {
      reason = "Overført / ny periode";
    } else {
      reason = "Avsluttet";
    }

    if (reason) { showReasonColumn = true; break; }
  }

  const reasonTh = $("permitReasonTh");
  if (reasonTh) reasonTh.style.display = showReasonColumn ? "" : "none";

  // --- Render card ---
  const card = safeEl("permitCard");
  card.classList.remove("hidden");

  if (now) {
    let rowDict = {};
    try { rowDict = now.row_json ? JSON.parse(now.row_json) : {}; } catch { rowDict = {}; }

    const artText = (now.art && String(now.art).trim()) ? now.art : (rowDict["ART"] ?? "");
    const artHtml = artText ? escapeHtml(artText) : "";

    const formal = (rowDict["FORMÅL"] ?? "").toString().trim();
    const produksjonsstadium = (rowDict["PRODUKSJONSSTADIUM"] ?? rowDict["PRODUKSJONSFORM"] ?? "").toString().trim();

    const kapRaw = (rowDict["TILL_KAP"] ?? "").toString().trim();
    const enh = (rowDict["TILL_ENHET"] ?? "").toString().trim();

    let kap = kapRaw;
    if (kapRaw && /^-?\d+([.,]\d+)?$/.test(kapRaw)) {
      const num = Number(kapRaw.replace(",", "."));
      if (Number.isFinite(num)) {
        kap = Number.isInteger(num) ? String(Math.trunc(num)) : String(num).replace(".", ",");
      }
    }
    const kapasitet = kap ? `${kap}${enh ? " " + enh : ""}` : "";

    const prodOmrRaw = (rowDict["PROD_OMR"] ?? "").toString().trim();
    const prodOmr = prodOmrRaw ? prodOmrRaw : "N/A";

    const grunnrente = Number(now.grunnrente_pliktig) === 1;
    const grunnPillClass = grunnrente ? "pill--blue" : "pill--yellow";
    const grunnPillText = grunnrente ? "Grunnrentepliktig" : "Ikke grunnrentepliktig";

    const ownerIdent = String(now.owner_identity ?? "").trim();

    card.innerHTML = `
      <div>
        <a href="${permitUrl}" target="_blank" rel="noopener noreferrer" class="permit-title-link">
          ${escapeHtml(now.permit_key)}
        </a>
      </div>

      <div class="pills">
        <span class="pill pill--green">Gjeldende status</span>
        <span class="pill ${grunnPillClass}">${grunnPillText}</span>
      </div>

      <div style="margin-top:10px">
        <div><span class="muted">Eier:</span> ${escapeHtml(now.owner_name)}</div>
        <div><span class="muted">Org.nr.:</span>
          ${ownerIdent
            ? `<a class="link" href="#/owner/${encodeURIComponent(ownerIdent)}">${escapeHtml(ownerIdent)}</a>`
            : `${escapeHtml(ownerIdent)}`
          }
        </div>

        ${artText ? `<div style="margin-top:8px"><span class="muted">Arter:</span> ${artHtml}</div>` : ""}

        <div style="margin-top:10px">
          <div><span class="muted">Formål:</span> ${escapeHtml(formal || "")}</div>
          <div><span class="muted">Produksjonsstadium:</span> ${escapeHtml(produksjonsstadium || "")}</div>
          <div><span class="muted">Tillatelseskapasitet:</span> ${escapeHtml(kapasitet || "")}</div>
          <div><span class="muted">Produksjonsområde:</span> ${escapeHtml(prodOmr)}</div>
        </div>
      </div>
    `;
  } else {
    // Ikke aktiv i permit_current -> bygg kort fra historikk
    const last = hist[hist.length - 1];
    const lastTo = iso10(last.valid_to);
    const tb = iso10(last.tidsbegrenset);

    let endText = "Ikke aktiv";
    if (lastTo) {
      endText = (tb && tb === lastTo)
        ? `Utløpt (tidsbegrenset ${tb})`
        : `Avsluttet (${lastTo})`;
    }

    const maxSnap = one(`SELECT MAX(snapshot_date) AS max_date FROM snapshots;`);
    const maxDate = maxSnap?.max_date ? String(maxSnap.max_date) : "";

    // Hent sist kjente art fra permit_snapshot (kun hvis kolonnen finnes)
    let artText = "";
    if (schema.permit_snapshot_has_art) {
      const lastSnap = one(`
        SELECT art AS art
        FROM permit_snapshot
        WHERE UPPER(REPLACE(TRIM(permit_key), ' ', '')) = ?
        ORDER BY snapshot_date DESC
        LIMIT 1;
      `, [permitKey]);

      artText = (lastSnap?.art && String(lastSnap.art).trim()) ? lastSnap.art : "";
    }
    const artHtml = artText ? escapeHtml(artText) : "";

    const lastOwnerIdent = String(last.owner_identity ?? "").trim();

    card.innerHTML = `
      <div>
        <a href="${permitUrl}" target="_blank" rel="noopener noreferrer" class="permit-title-link">
          ${escapeHtml(permitKey)}
        </a>
      </div>
      <div class="muted">Ikke aktiv i siste snapshot${maxDate ? ` (${escapeHtml(displayDate(maxDate))})` : ""} • ${escapeHtml(endText)}</div>

      <div style="margin-top:8px">
        <div><span class="muted">Siste kjente eier:</span> ${escapeHtml(last.owner_name || "")}</div>
        <div><span class="muted">Org.nr.:</span>
          ${lastOwnerIdent
            ? `<a class="link" href="#/owner/${encodeURIComponent(lastOwnerIdent)}">${escapeHtml(lastOwnerIdent)}</a>`
            : `${escapeHtml(lastOwnerIdent)}`
          }
        </div>
        ${tb ? `<div><span class="muted">Tidsbegrenset:</span> ${escapeHtml(tb)}</div>` : ""}
        ${artText ? `<div style="margin-top:8px"><span class="muted">Arter:</span> ${artHtml}</div>` : ""}
      </div>
    `;
  }

  // --- Render history table ---
  const tbody = safeEl("permitHistoryTable").querySelector("tbody");
  tbody.innerHTML = "";

  for (let i = 0; i < hist.length; i++) {
    const r = hist[i];
    const next = hist[i + 1] || null;

    const validTo = iso10(r.valid_to);
    const tb = iso10(r.tidsbegrenset);

    let reason = "";
    if (!validTo) {
      reason = "";
    } else if (tb && tb === validTo) {
      reason = `Utløpt (tidsbegrenset ${tb})`;
    } else if (next) {
      reason = "Overført / ny periode";
    } else {
      reason = "Avsluttet";
    }
    if (!reason) reason = "--";

    const vf = displayDate(r.valid_from);
    const vtLabel = (r.valid_to_label === "Aktiv") ? "Aktiv" : displayDate(r.valid_to_label);

    const ident = String(r.owner_identity ?? "").trim();

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(vf)}</td>
      <td>${escapeHtml(vtLabel)}</td>
      ${showReasonColumn ? `<td class="muted">${escapeHtml(reason)}</td>` : ""}
      <td>${escapeHtml(r.owner_name || "")}</td>
      <td>${ident ? `<a class="link" href="#/owner/${encodeURIComponent(ident)}">${escapeHtml(ident)}</a>` : ""}</td>
    `;
    tbody.appendChild(tr);
  }
}

// --- OWNER view ---
function renderOwner(ownerIdentity) {
  setActiveTab("tab-owner");
  showView("view-owner");

  clearOwnerView();

  if (ownerIdentity != null) safeEl("ownerInput").value = String(ownerIdentity);

  const inputEl = safeEl("ownerInput");
  const identTrim = String(ownerIdentity || "").trim();

  if (!identTrim) {
    safeEl("ownerEmpty").textContent =
      "Skriv et org.nr. (9 siffer) i feltet over, eller klikk en eier fra Nå-status/historikk.";
    return;
  }

  if (!isNineDigits(identTrim)) {
    safeEl("ownerEmpty").textContent =
      "Ugyldig org.nr. Skriv et tall med 9 siffer.";
    return;
  }

  ownerIdentity = identTrim.replace(/\s+/g, "");
  inputEl.value = ownerIdentity;

  const stats = one(`
    SELECT
      owner_identity AS owner_identity,
      MAX(owner_name) AS owner_name,
      SUM(CASE WHEN valid_to IS NULL OR valid_to = '' THEN 1 ELSE 0 END) AS active_permits,
      COUNT(*) AS total_periods
    FROM ownership_history
    WHERE owner_identity = ?
    GROUP BY owner_identity;
  `, [ownerIdentity]);

  if (!stats) {
    safeEl("ownerEmpty").textContent = `Fant ikke org.nr.: ${ownerIdentity}`;
    return;
  }

  const card = safeEl("ownerCard");
  card.classList.remove("hidden");
  card.innerHTML = `
    <div><strong>${escapeHtml(stats.owner_name || "(ukjent)")}</strong></div>
    <div class="muted">Org.nr.: ${escapeHtml(stats.owner_identity)}</div>
    <div style="margin-top:8px" class="muted">
      Aktive tillatelser: ${stats.active_permits} • Historiske perioder: ${stats.total_periods}
    </div>
  `;

  const active = execAll(`
    SELECT
      permit_key AS permit_key,
      ${schema.permit_current_has_art ? "art AS art" : "NULL AS art"},
      row_json AS row_json
    FROM permit_current
    WHERE owner_identity = ?
    ORDER BY permit_key;
  `, [ownerIdentity]);

  const activeBody = safeEl("ownerActiveTable").querySelector("tbody");
  activeBody.innerHTML = "";

  for (const r of active) {
    const rowDict = r.row_json
      ? (() => { try { return JSON.parse(r.row_json); } catch { return {}; } })()
      : {};

    const art = (r.art && String(r.art).trim()) ? r.art : (rowDict["ART"] ?? "");
    const formal = rowDict["FORMÅL"] ?? "";
    const produksjonsform = rowDict["PRODUKSJONSFORM"] ?? "";
    const kap = rowDict["TILL_KAP"] ?? "";
    const enh = rowDict["TILL_ENHET"] ?? "";
    const prodOmr = rowDict["PROD_OMR"] ?? "";

    const kapasitet = String(kap).trim()
      ? `${String(kap).trim()}${String(enh).trim() ? " " + String(enh).trim() : ""}`
      : "";

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(String(r.permit_key ?? ""))}">${escapeHtml(r.permit_key)}</a></td>
      <td>${escapeHtml(art)}</td>
      <td>${escapeHtml(formal)}</td>
      <td>${escapeHtml(produksjonsform)}</td>
      <td>${escapeHtml(kapasitet)}</td>
      <td>${escapeHtml(prodOmr)}</td>
    `;
    activeBody.appendChild(tr);
  }

  const hist = execAll(`
    SELECT
      permit_key AS permit_key,
      valid_from AS valid_from,
      valid_to   AS valid_to,
      COALESCE(NULLIF(valid_to,''), 'Aktiv') AS valid_to_label,
      owner_name AS owner_name,
      owner_orgnr AS owner_orgnr,
      tidsbegrenset AS tidsbegrenset
    FROM ownership_history
    WHERE owner_identity = ?
    ORDER BY permit_key, date(valid_from), id;
  `, [ownerIdentity]);

  const histBody = safeEl("ownerHistoryTable").querySelector("tbody");
  histBody.innerHTML = "";

  for (let i = 0; i < hist.length; i++) {
    const r = hist[i];
    const next = hist[i + 1] || null;

    const validTo = iso10(r.valid_to);
    const tb = iso10(r.tidsbegrenset);
    const hasNextSamePermit = Boolean(next && next.permit_key === r.permit_key);

    let reason = "";
    if (!validTo) {
      reason = "";
    } else if (tb && tb === validTo) {
      reason = `Utløpt (tidsbegrenset ${tb})`;
    } else if (hasNextSamePermit) {
      reason = "Overført / ny periode";
    } else {
      reason = "Avsluttet";
    }
    if (!reason) reason = "--";

    const vf = displayDate(r.valid_from);
    const vtLabel = (r.valid_to_label === "Aktiv") ? "Aktiv" : displayDate(r.valid_to_label);

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(String(r.permit_key ?? ""))}">${escapeHtml(r.permit_key)}</a></td>
      <td>${escapeHtml(vf)}</td>
      <td>${escapeHtml(vtLabel)}</td>
      <td class="muted">${escapeHtml(reason)}</td>
      <td>${escapeHtml(r.owner_name)}</td>
      <td>${escapeHtml(r.owner_orgnr || "")}</td>
    `;
    histBody.appendChild(tr);
  }
}

// --- routing ---
function parseHash() {
  const h = (location.hash || "#/now").replace(/^#\/?/, "");
  const parts = h.split("/").filter(Boolean);

  if (parts.length === 0 || parts[0] === "now") return { view: "now" };

  if (parts[0] === "permit") {
    const key = parts[1] ? decodeURIComponent(parts[1]) : null;
    return { view: "permit", key };
  }

  if (parts[0] === "owner") {
    const ident = parts[1] ? decodeURIComponent(parts[1]) : null;
    return { view: "owner", ident };
  }

  return { view: "now" };
}

function renderRoute() {
  if (!db) return;
  const r = parseHash();
  if (r.view === "now") return renderNow();
  if (r.view === "permit") return renderPermit(r.key);
  if (r.view === "owner") return renderOwner(r.ident);
  renderNow();
}

// --- events ---
function wireEvents() {
  window.addEventListener("hashchange", () => renderRoute());

  // NOW search (debounced)
  let nowTimer = null;
  safeEl("nowSearch").addEventListener("input", () => {
    clearTimeout(nowTimer);
    nowTimer = setTimeout(() => renderNow(), 80);
  });
  safeEl("onlyGrunnrente").addEventListener("change", () => renderNow());

  // NOW sorting via data-sort headers
  document.querySelectorAll("#nowTable thead th[data-sort]").forEach(th => {
    th.addEventListener("click", () => {
      const key = th.getAttribute("data-sort");
      if (!key) return;

      if (sortState.now.key === key) sortState.now.dir *= -1;
      else { sortState.now.key = key; sortState.now.dir = 1; }

      renderNow();
    });
  });

  // PERMIT actions
  safeEl("permitGo").addEventListener("click", () => {
    const raw = safeEl("permitInput").value;

    if (!String(raw || "").trim()) {
      clearPermitView();
      setPermitEmptyStateContent({
        icon: "🔍",
        title: "Søk etter tillatelse",
        text: "Skriv et tillatelsesnr. (f.eks. H-F-0910)."
      });
      setPermitEmptyStateVisible(true);
      return;
    }

    if (isNineDigits(raw)) {
      clearPermitView();
      setPermitEmptyStateContent({
        icon: "ℹ️",
        title: "Dette ser ut som org.nr.",
        text: "Dette er et org.nr. (9 siffer). Bruk fanen Innehaver for org.nr."
      });
      setPermitEmptyStateVisible(true);
      return;
    }

    toHashPermit(raw);
  });

  safeEl("permitInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") safeEl("permitGo").click();
  });

  // Tøm permit-visning når input slettes
  safeEl("permitInput").addEventListener("input", (e) => {
    const v = String(e.target.value || "").trim();
    if (!v) {
      clearPermitView();
      setPermitEmptyStateContent({
        icon: "🔍",
        title: "Søk etter tillatelse",
        text: "Skriv et tillatelsesnummer i feltet over for å se detaljer og historikk."
      });
      setPermitEmptyStateVisible(true);
      location.hash = "#/permit";
    }
  });

  // OWNER actions
  safeEl("ownerGo").addEventListener("click", () => {
    const ident = safeEl("ownerInput").value.trim();
    if (!ident) {
      clearOwnerView();
      safeEl("ownerEmpty").textContent = "Skriv et org.nr. (9 siffer).";
      return;
    }
    if (!isNineDigits(ident)) {
      clearOwnerView();
      safeEl("ownerEmpty").textContent = "Ugyldig org.nr. Skriv et tall med 9 siffer.";
      return;
    }
    toHashOwner(ident);
  });

  safeEl("ownerInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") safeEl("ownerGo").click();
  });

  // Tøm owner-visning når input slettes
  safeEl("ownerInput").addEventListener("input", (e) => {
    const v = e.target.value.trim();
    if (!v) {
      clearOwnerView();
      safeEl("ownerEmpty").textContent =
        "Skriv et org.nr. (9 siffer) i feltet over, eller klikk en eier fra Nå-status/historikk.";
      location.hash = "#/owner";
    }
  });
}

function showError(err) {
  console.error(err);
  setStatus("Feil ved lasting", "bad");
  setMeta(String(err?.message || err));
}

// --- main ---
(async function main() {
  try {
    wireEvents();
    if (!location.hash) toHashNow();
    await loadDatabase();
  } catch (e) {
    showError(e);
  }
})();
