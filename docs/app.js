let db = null;
let latestMeta = null;

// -------------------- UI helpers --------------------
function setStatus(msg) {
  document.getElementById("status").textContent = msg;
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
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderKeyValueTableHtml(obj, title = null) {
  const keys = Object.keys(obj || {});
  if (!obj || keys.length === 0) {
    return "<p>Ingen data.</p>";
  }

  let html = "";
  if (title) html += `<h4>${escapeHtml(title)}</h4>`;
  html += "<table><thead><tr><th>Felt</th><th>Verdi</th></tr></thead><tbody>";
  for (const key of keys.sort()) {
    const val = obj[key];
    html += `<tr>
      <td><code>${escapeHtml(key)}</code></td>
      <td>${val === null ? "" : escapeHtml(String(val))}</td>
    </tr>`;
  }
  html += "</tbody></table>";
  return html;
}

function renderDetails(containerId, obj, title = null) {
  const el = document.getElementById(containerId);
  el.innerHTML = renderKeyValueTableHtml(obj, title);
}

function clearHolderNowUI() {
  document.getElementById("holderNowResult").innerHTML = "";
  document.getElementById("holderNowDetails").innerHTML = "";
}
function clearHolderHistUI() {
  document.getElementById("holderHistResult").innerHTML = "";
  document.getElementById("holderHistDetails").innerHTML = "";
}
function clearPermitNowUI() {
  document.getElementById("permitNowResult").innerHTML = "";
  document.getElementById("permitNowDetails").innerHTML = "";
}
function clearPermitHistUI() {
  document.getElementById("permitHistResult").innerHTML = "";
  document.getElementById("permitHistDetails").innerHTML = "";
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

// -------------------- Snapshot lookups --------------------
function getSnapshotRowJson(permitId, holderId = null, preferDate = null) {
  // 1) Prøv preferDate
  if (preferDate) {
    const q1 = holderId
      ? `SELECT row_json FROM permit_snapshot
         WHERE snapshot_date=$d AND permit_id=$p AND holder_id=$h LIMIT 1`
      : `SELECT row_json FROM permit_snapshot
         WHERE snapshot_date=$d AND permit_id=$p LIMIT 1`;

    const rows1 = runQuery(q1, holderId ? { $d: preferDate, $p: permitId, $h: holderId } : { $d: preferDate, $p: permitId });
    if (rows1.length > 0) return rows1[0][0];
  }

  // 2) Fall back: nyeste
  const q2 = holderId
    ? `SELECT row_json FROM permit_snapshot
       WHERE permit_id=$p AND holder_id=$h ORDER BY snapshot_date DESC LIMIT 1`
    : `SELECT row_json FROM permit_snapshot
       WHERE permit_id=$p ORDER BY snapshot_date DESC LIMIT 1`;

  const rows2 = runQuery(q2, holderId ? { $p: permitId, $h: holderId } : { $p: permitId });
  if (rows2.length > 0) return rows2[0][0];
  return null;
}

function tryExtract(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null && String(obj[k]).trim() !== "") return String(obj[k]);
  }
  return "";
}

// -------------------- “kortkort” for liste-elementer (VERTIKALT) --------------------
function renderVerticalCards(containerId, cardsHtml) {
  const el = document.getElementById(containerId);
  if (!cardsHtml || cardsHtml.length === 0) {
    el.innerHTML = "<p>Ingen treff.</p>";
    return;
  }
  el.innerHTML = cardsHtml.join("\n<hr />\n");
}

function cardFromSummary(summaryObj, permitId, holderId, preferDate, detailsTargetId, heading) {
  // summaryObj -> key/value tabell + Detaljer-knapp
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
  return `
    ${title}
    <div style="display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin: 8px 0;">
      ${detailsBtn}
    </div>
    ${kv}
  `;
}

// -------------------- View 1: Holder -> permits (NOW) --------------------
document.getElementById("holderNowBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderNowInput").value.trim();
  if (!holderId) { clearHolderNowUI(); return; }

  const q = `
    SELECT permit_id, valid_from
    FROM ownership_intervals
    WHERE holder_id=$h AND valid_to IS NULL
    ORDER BY permit_id
  `;
  const rows = runQuery(q, { $h: holderId });

  if (rows.length === 0) {
    document.getElementById("holderNowResult").innerHTML = "<p>Ingen aktive tillatelser funnet for denne innehaveren.</p>";
    document.getElementById("holderNowDetails").innerHTML = "";
    return;
  }

  const cards = rows.map(([permitId, validFrom]) => {
    const rowJson = getSnapshotRowJson(permitId, holderId, latestMeta.snapshot_date);
    let navn = "", lok = "", art = "", formål = "";
    if (rowJson) {
      const obj = JSON.parse(rowJson);
      navn = tryExtract(obj, ["NAVN"]);
      lok = tryExtract(obj, ["LOK_NAVN", "LOK_PLASS", "LOK_KOM"]);
      art = tryExtract(obj, ["ART"]);
      formål = tryExtract(obj, ["FORMÅL"]);
    }
    const summary = {
      "TILL_NR": permitId,
      "NAVN": navn || "(mangler)",
      "LOK": lok || "",
      "ART": art || "",
      "FORMÅL": formål || "",
      "Fra": validFrom
    };
    return cardFromSummary(summary, permitId, holderId, latestMeta.snapshot_date, "holderNowDetails", `Tillatelse ${permitId}`);
  });

  renderVerticalCards("holderNowResult", cards);
  document.getElementById("holderNowDetails").innerHTML = "";
});

// -------------------- View 2: Holder -> permits (HISTORY) --------------------
document.getElementById("holderHistBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderHistInput").value.trim();
  if (!holderId) { clearHolderHistUI(); return; }

  const q = `
    SELECT permit_id, valid_from, COALESCE(valid_to, 'NÅ') AS valid_to
    FROM ownership_intervals
    WHERE holder_id=$h
    ORDER BY permit_id, valid_from
  `;
  const rows = runQuery(q, { $h: holderId });

  if (rows.length === 0) {
    document.getElementById("holderHistResult").innerHTML = "<p>Ingen historikk funnet for denne innehaveren.</p>";
    document.getElementById("holderHistDetails").innerHTML = "";
    return;
  }

  const cards = rows.map(([permitId, from, to]) => {
    const rowJson = getSnapshotRowJson(permitId, holderId, from);
    let navn = "", lok = "", art = "", formål = "";
    if (rowJson) {
      const obj = JSON.parse(rowJson);
      navn = tryExtract(obj, ["NAVN"]);
      lok = tryExtract(obj, ["LOK_NAVN", "LOK_PLASS", "LOK_KOM"]);
      art = tryExtract(obj, ["ART"]);
      formål = tryExtract(obj, ["FORMÅL"]);
    }
    const summary = {
      "TILL_NR": permitId,
      "NAVN": navn || "(mangler)",
      "LOK": lok || "",
      "ART": art || "",
      "FORMÅL": formål || "",
      "Fra": from,
      "Til": to
    };
    return cardFromSummary(summary, permitId, holderId, from, "holderHistDetails", `Tillatelse ${permitId} (${from} → ${to})`);
  });

  renderVerticalCards("holderHistResult", cards);
  document.getElementById("holderHistDetails").innerHTML = "";
});

// -------------------- View 3: Permit -> owner (NOW) --------------------
document.getElementById("permitNowBtn").addEventListener("click", () => {
  const permitId = document.getElementById("permitNowInput").value.trim();
  if (!permitId) { clearPermitNowUI(); return; }

  // Snapshot (for navn osv.)
  const rowJson = getSnapshotRowJson(permitId, null, latestMeta.snapshot_date);
  if (!rowJson) {
    document.getElementById("permitNowResult").innerHTML = "<p>Fant ingen tillatelse med dette nummeret i snapshot-tabellen.</p>";
    document.getElementById("permitNowDetails").innerHTML = "";
    return;
  }

  const obj = JSON.parse(rowJson);
  const navn = tryExtract(obj, ["NAVN"]);
  const lok = tryExtract(obj, ["LOK_NAVN", "LOK_PLASS", "LOK_KOM"]);
  const art = tryExtract(obj, ["ART"]);
  const formål = tryExtract(obj, ["FORMÅL"]);
  const holderFromSnap = tryExtract(obj, ["ORG.NR/PERS.NR"]);

  // Eier fra historikk-tabellen (hvis finnes)
  const qOwner = `
    SELECT holder_id, valid_from
    FROM ownership_intervals
    WHERE permit_id=$p AND valid_to IS NULL
    ORDER BY valid_from DESC
    LIMIT 1
  `;
  const own = runQuery(qOwner, { $p: permitId });
  const eier = own.length > 0 ? own[0][0] : "";
  const eierFra = own.length > 0 ? own[0][1] : "";

  const summary = {
    "TILL_NR": permitId,
    "NAVN": navn || "(mangler)",
    "LOK": lok || "",
    "ART": art || "",
    "FORMÅL": formål || "",
    "Snapshot": latestMeta.snapshot_date,
    "ORG.NR/PERS.NR (fra snapshot)": holderFromSnap || "(mangler)",
    "Eier (fra historikk-tabell)": eier ? `${eier} (fra ${eierFra})` : "(ikke funnet)"
  };

  // Render som ett vertikalt “kort” med Detaljer-knapp som de andre
  const card = cardFromSummary(summary, permitId, eier || holderFromSnap || "", latestMeta.snapshot_date, "permitNowDetails", `Tillatelse ${permitId}`);
  renderVerticalCards("permitNowResult", [card]);
  document.getElementById("permitNowDetails").innerHTML = "";
});

// (Valgfritt) behold knappen i HTML, men la den gjøre det samme som “Detaljer”
const permitNowDetailsBtn = document.getElementById("permitNowDetailsBtn");
if (permitNowDetailsBtn) {
  permitNowDetailsBtn.addEventListener("click", () => {
    const permitId = document.getElementById("permitNowInput").value.trim();
    if (!permitId) { clearPermitNowUI(); return; }
    const rowJson = getSnapshotRowJson(permitId, null, latestMeta.snapshot_date);
    if (!rowJson) {
      document.getElementById("permitNowDetails").innerHTML = "<p>Fant ingen detaljer for denne tillatelsen.</p>";
      return;
    }
    renderDetails("permitNowDetails", JSON.parse(rowJson), `Detaljer for ${permitId}`);
  });
}

// -------------------- View 4: Permit -> owners (HISTORY) --------------------
document.getElementById("permitHistBtn").addEventListener("click", () => {
  const permitId = document.getElementById("permitHistInput").value.trim();
  if (!permitId) { clearPermitHistUI(); return; }

  const q = `
    SELECT holder_id, valid_from, COALESCE(valid_to, 'NÅ') AS valid_to
    FROM ownership_intervals
    WHERE permit_id=$p
    ORDER BY valid_from
  `;
  const rows = runQuery(q, { $p: permitId });

  if (rows.length === 0) {
    document.getElementById("permitHistResult").innerHTML = "<p>Ingen historikk funnet for denne tillatelsen.</p>";
    document.getElementById("permitHistDetails").innerHTML = "";
    return;
  }

  const cards = rows.map(([holderId, from, to]) => {
    const rowJson = getSnapshotRowJson(permitId, holderId, from);
    let navn = "";
    if (rowJson) {
      const obj = JSON.parse(rowJson);
      navn = tryExtract(obj, ["NAVN"]);
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

// -------------------- Global “Detaljer”-knapper (event delegation) --------------------
document.addEventListener("click", (e) => {
  const btn = e.target.closest(".details-btn");
  if (!btn) return;

  const targetId = btn.getAttribute("data-target");
  const permitId = btn.getAttribute("data-permit");
  const holderId = btn.getAttribute("data-holder") || null;
  const date = btn.getAttribute("data-date") || null;

  const rowJson = getSnapshotRowJson(permitId, holderId && holderId.trim() !== "" ? holderId : null, date && date.trim() !== "" ? date : null);
  if (!rowJson) {
    document.getElementById(targetId).innerHTML = "<p>Fant ingen snapshot-rad for denne kombinasjonen (tillatelse/innehaver/dato).</p>";
    return;
  }

  renderDetails(targetId, JSON.parse(rowJson), `Detaljer for ${permitId}`);
});

// -------------------- Clear-on-empty + Enter-to-search for all inputs --------------------
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
  } catch (e) {
    console.error(e);
    setStatus("Feil: " + e.message);
  }
})();
