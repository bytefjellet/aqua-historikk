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

function renderKeyValueTable(containerId, obj) {
  const el = document.getElementById(containerId);
  if (!obj || Object.keys(obj).length === 0) {
    el.innerHTML = "<p>Ingen detaljer funnet.</p>";
    return;
  }

  let html = "<table><thead><tr><th>Felt</th><th>Verdi</th></tr></thead><tbody>";
  for (const key of Object.keys(obj).sort()) {
    const val = obj[key];
    html += `<tr>
      <td><code>${escapeHtml(key)}</code></td>
      <td>${val === null ? "" : escapeHtml(String(val))}</td>
    </tr>`;
  }
  html += "</tbody></table>";
  el.innerHTML = html;
}

function escapeHtml(s) {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
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
    const cols = stmt.getColumnNames();
    return { cols, rows };
  } finally {
    stmt.free();
  }
}

// -------------------- Snapshot lookups --------------------
function getSnapshotRowJson(permitId, holderId = null, preferDate = null) {
  // Prøv først å finne row_json på preferDate (om angitt), ellers fall tilbake til nyeste snapshot.
  if (preferDate) {
    const q1 = holderId
      ? `
        SELECT row_json
        FROM permit_snapshot
        WHERE snapshot_date = $d AND permit_id = $p AND holder_id = $h
        LIMIT 1
      `
      : `
        SELECT row_json
        FROM permit_snapshot
        WHERE snapshot_date = $d AND permit_id = $p
        LIMIT 1
      `;
    const p1 = holderId ? { $d: preferDate, $p: permitId, $h: holderId } : { $d: preferDate, $p: permitId };
    const r1 = runQuery(q1, p1);
    if (r1.rows && r1.rows.length > 0) return r1.rows[0][0];
  }

  const q2 = holderId
    ? `
      SELECT row_json
      FROM permit_snapshot
      WHERE permit_id = $p AND holder_id = $h
      ORDER BY snapshot_date DESC
      LIMIT 1
    `
    : `
      SELECT row_json
      FROM permit_snapshot
      WHERE permit_id = $p
      ORDER BY snapshot_date DESC
      LIMIT 1
    `;
  const p2 = holderId ? { $p: permitId, $h: holderId } : { $p: permitId };
  const r2 = runQuery(q2, p2);
  if (r2.rows && r2.rows.length > 0) return r2.rows[0][0];
  return null;
}

function tryExtract(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null && String(obj[k]).trim() !== "") return String(obj[k]);
  }
  return "";
}

// -------------------- View 1: Holder -> permits (NOW) --------------------
document.getElementById("holderNowBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderNowInput").value.trim();
  if (!holderId) {
    clearHolderNowUI();
    return;
  }

  // Kortvisning: alle aktive tillatelser for innehaver, med litt snapshot-info (NAVN/LOK_NAVN/ART osv.)
  const q = `
    SELECT permit_id, valid_from
    FROM ownership_intervals
    WHERE holder_id = $h AND valid_to IS NULL
    ORDER BY permit_id
  `;
  const r = runQuery(q, { $h: holderId });

  if (!r.rows || r.rows.length === 0) {
    document.getElementById("holderNowResult").innerHTML = "<p>Ingen aktive tillatelser funnet for denne innehaveren.</p>";
    document.getElementById("holderNowDetails").innerHTML = "";
    return;
  }

  // Bygg en tabell med “Detaljer”-knapp per rad
  const rows = r.rows.map(([permitId, validFrom]) => {
    // hent nyeste snapshot for permit+holder (helst i dag)
    const rowJson = getSnapshotRowJson(permitId, holderId, latestMeta.snapshot_date);
    let navn = "", lok = "", art = "", formål = "";
    if (rowJson) {
      const obj = JSON.parse(rowJson);
      navn = tryExtract(obj, ["NAVN"]);
      lok = tryExtract(obj, ["LOK_NAVN", "LOK_PLASS", "LOK_KOM"]);
      art = tryExtract(obj, ["ART"]);
      formål = tryExtract(obj, ["FORMÅL"]);
    }
    return [
      permitId,
      navn || "(mangler)",
      lok || "",
      art || "",
      formål || "",
      validFrom,
      `<button class="details-btn" data-target="holderNowDetails" data-permit="${escapeHtml(permitId)}" data-holder="${escapeHtml(holderId)}" data-date="${escapeHtml(latestMeta.snapshot_date)}">Detaljer</button>`
    ];
  });

  renderTable("holderNowResult",
    ["TILL_NR", "NAVN", "LOK", "ART", "FORMÅL", "Fra", ""],
    rows
  );
  document.getElementById("holderNowDetails").innerHTML = "";
});

// -------------------- View 2: Holder -> permits (HISTORY) --------------------
document.getElementById("holderHistBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderHistInput").value.trim();
  if (!holderId) {
    clearHolderHistUI();
    return;
  }

  // Kortvisning: intervaller for innehaver
  const q = `
    SELECT permit_id, valid_from, COALESCE(valid_to, 'NÅ') AS valid_to
    FROM ownership_intervals
    WHERE holder_id = $h
    ORDER BY permit_id, valid_from
  `;
  const r = runQuery(q, { $h: holderId });

  if (!r.rows || r.rows.length === 0) {
    document.getElementById("holderHistResult").innerHTML = "<p>Ingen historikk funnet for denne innehaveren.</p>";
    document.getElementById("holderHistDetails").innerHTML = "";
    return;
  }

  const rows = r.rows.map(([permitId, from, to]) => {
    // prøv å hente snapshot ved from (best match)
    const rowJson = getSnapshotRowJson(permitId, holderId, from);
    let navn = "", lok = "", art = "", formål = "";
    if (rowJson) {
      const obj = JSON.parse(rowJson);
      navn = tryExtract(obj, ["NAVN"]);
      lok = tryExtract(obj, ["LOK_NAVN", "LOK_PLASS", "LOK_KOM"]);
      art = tryExtract(obj, ["ART"]);
      formål = tryExtract(obj, ["FORMÅL"]);
    }
    return [
      permitId,
      navn || "(mangler)",
      lok || "",
      art || "",
      formål || "",
      from,
      to,
      `<button class="details-btn" data-target="holderHistDetails" data-permit="${escapeHtml(permitId)}" data-holder="${escapeHtml(holderId)}" data-date="${escapeHtml(from)}">Detaljer</button>`
    ];
  });

  renderTable("holderHistResult",
    ["TILL_NR", "NAVN", "LOK", "ART", "FORMÅL", "Fra", "Til", ""],
    rows
  );
  document.getElementById("holderHistDetails").innerHTML = "";
});

// -------------------- View 3: Permit -> owner (NOW) --------------------
document.getElementById("permitNowBtn").addEventListener("click", () => {
  const permitId = document.getElementById("permitNowInput").value.trim();
  if (!permitId) {
    clearPermitNowUI();
    return;
  }

  // Kortvisning: hent snapshot (ID+NAVN + litt) + eier hvis ownership_intervals har det
  const rowJson = getSnapshotRowJson(permitId, null, latestMeta.snapshot_date);
  if (!rowJson) {
    document.getElementById("permitNowResult").innerHTML = "<p>Fant ingen tillatelse med dette nummeret i snapshot-tabellen.</p>";
    document.getElementById("permitNowDetails").innerHTML = "";
    return;
  }

  const obj = JSON.parse(rowJson);
  const navn = tryExtract(obj, ["NAVN"]);
  const holderFromSnap = tryExtract(obj, ["ORG.NR/PERS.NR"]);
  const lok = tryExtract(obj, ["LOK_NAVN", "LOK_PLASS", "LOK_KOM"]);
  const art = tryExtract(obj, ["ART"]);
  const formål = tryExtract(obj, ["FORMÅL"]);

  // eier fra historikk-tabell (hvis finnes)
  let eier = "", fra = "";
  const qOwner = `
    SELECT holder_id, valid_from
    FROM ownership_intervals
    WHERE permit_id = $p AND valid_to IS NULL
    ORDER BY valid_from DESC
    LIMIT 1
  `;
  const own = runQuery(qOwner, { $p: permitId });
  if (own.rows && own.rows.length > 0) {
    eier = own.rows[0][0];
    fra = own.rows[0][1];
  }

  const rows = [
    ["TILL_NR", permitId],
    ["NAVN", navn || "(mangler)"],
    ["LOK", lok || ""],
    ["ART", art || ""],
    ["FORMÅL", formål || ""],
    ["Snapshot", latestMeta.snapshot_date],
    ["ORG.NR/PERS.NR (fra snapshot)", holderFromSnap || "(mangler)"],
    ["Eier (fra historikk-tabell)", eier ? `${eier} (fra ${fra})` : "(ikke funnet)"]
  ];
  renderTable("permitNowResult", ["Felt", "Verdi"], rows);

  // nytt søk -> tøm detaljer
  document.getElementById("permitNowDetails").innerHTML = "";
});

document.getElementById("permitNowDetailsBtn").addEventListener("click", () => {
  const permitId = document.getElementById("permitNowInput").value.trim();
  if (!permitId) {
    clearPermitNowUI();
    return;
  }
  const rowJson = getSnapshotRowJson(permitId, null, latestMeta.snapshot_date);
  if (!rowJson) {
    document.getElementById("permitNowDetails").innerHTML = "<p>Fant ingen detaljer for denne tillatelsen.</p>";
    return;
  }
  renderKeyValueTable("permitNowDetails", JSON.parse(rowJson));
});

// -------------------- View 4: Permit -> owners (HISTORY) --------------------
document.getElementById("permitHistBtn").addEventListener("click", () => {
  const permitId = document.getElementById("permitHistInput").value.trim();
  if (!permitId) {
    clearPermitHistUI();
    return;
  }

  // Kortvisning: eier-intervaller
  const q = `
    SELECT holder_id, valid_from, COALESCE(valid_to, 'NÅ') AS valid_to
    FROM ownership_intervals
    WHERE permit_id = $p
    ORDER BY valid_from
  `;
  const r = runQuery(q, { $p: permitId });

  if (!r.rows || r.rows.length === 0) {
    document.getElementById("permitHistResult").innerHTML = "<p>Ingen historikk funnet for denne tillatelsen.</p>";
    document.getElementById("permitHistDetails").innerHTML = "";
    return;
  }

  const rows = r.rows.map(([holderId, from, to]) => {
    // prøv å hente snapshot ved from for å få NAVN (ofte innehavers navn)
    const rowJson = getSnapshotRowJson(permitId, holderId, from);
    let navn = "";
    if (rowJson) {
      const obj = JSON.parse(rowJson);
      navn = tryExtract(obj, ["NAVN"]);
    }
    return [
      holderId,
      navn || "(mangler)",
      from,
      to,
      `<button class="details-btn" data-target="permitHistDetails" data-permit="${escapeHtml(permitId)}" data-holder="${escapeHtml(holderId)}" data-date="${escapeHtml(from)}">Detaljer</button>`
    ];
  });

  renderTable("permitHistResult", ["ORG.NR/PERS.NR", "NAVN", "Fra", "Til", ""], rows);
  document.getElementById("permitHistDetails").innerHTML = "";
});

// -------------------- Global “Detaljer”-knapper (event delegation) --------------------
document.addEventListener("click", (e) => {
  const btn = e.target.closest(".details-btn");
  if (!btn) return;

  const targetId = btn.getAttribute("data-target");
  const permitId = btn.getAttribute("data-permit");
  const holderId = btn.getAttribute("data-holder");
  const date = btn.getAttribute("data-date"); // vi prøver å hente snapshot ved denne datoen

  const rowJson = getSnapshotRowJson(permitId, holderId || null, date || null);
  if (!rowJson) {
    document.getElementById(targetId).innerHTML = "<p>Fant ingen snapshot-rad for denne kombinasjonen (tillatelse/innehaver/dato).</p>";
    return;
  }
  renderKeyValueTable(targetId, JSON.parse(rowJson));
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
