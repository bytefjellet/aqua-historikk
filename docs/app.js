let db = null;
let latestMeta = null;

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
      <td><code>${key}</code></td>
      <td>${val === null ? "" : String(val)}</td>
    </tr>`;
  }
  html += "</tbody></table>";
  el.innerHTML = html;
}


async function loadLatestJson() {
  const r = await fetch("./latest.json", { cache: "no-store" });
  if (!r.ok) throw new Error("Klarte ikke å laste latest.json");
  return await r.json();
}

async function loadSqlJs() {
  // sql-wasm.js forventer å finne wasm-fila; vi peker den mot CDN.
  return await initSqlJs({
    locateFile: (file) => `https://cdn.jsdelivr.net/npm/sql.js@1.10.3/dist/${file}`
  });
}

async function downloadAndOpenDb(sqlJs) {
  const url = latestMeta.sqlite_gz_url;
  setStatus(`Laster database (${latestMeta.snapshot_date})…`);
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error("Klarte ikke å laste latest.sqlite.gz fra Release");

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

// ---- SØK 1: Innehaver -> tillatelser (i dag)
document.getElementById("holderNowBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderNowInput").value.trim();
  if (!holderId) return;

  const q = `
    SELECT permit_id AS tillatelse, valid_from AS fra
    FROM ownership_intervals
    WHERE holder_id = $holder AND valid_to IS NULL
    ORDER BY permit_id
  `;
  const { cols, rows } = runQuery(q, { $holder: holderId });
  renderTable("holderNowResult", cols, rows);
});

// ---- SØK 2: Innehaver -> tillatelser (historikk)
document.getElementById("holderHistBtn").addEventListener("click", () => {
  const holderId = document.getElementById("holderHistInput").value.trim();
  if (!holderId) return;

  const q = `
    SELECT permit_id AS tillatelse, valid_from AS fra, COALESCE(valid_to, 'NÅ') AS til
    FROM ownership_intervals
    WHERE holder_id = $holder
    ORDER BY permit_id, valid_from
  `;
  const { cols, rows } = runQuery(q, { $holder: holderId });
  renderTable("holderHistResult", cols, rows);
});

// ---- SØK 3: Tillatelse -> eier (i dag)
document.getElementById("permitNowBtn").addEventListener("click", () => {
  const permitId = document.getElementById("permitNowInput").value.trim();
  if (!permitId) return;

  const q = `
    SELECT holder_id AS eier, valid_from AS fra
    FROM ownership_intervals
    WHERE permit_id = $permit AND valid_to IS NULL
    ORDER BY valid_from DESC
    LIMIT 1
  `;
  const { cols, rows } = runQuery(q, { $permit: permitId });
  renderTable("permitNowResult", cols, rows);
});

// ---- SØK 4: Tillatelse -> eier (historikk)
document.getElementById("permitHistBtn").addEventListener("click", () => {
  const permitId = document.getElementById("permitHistInput").value.trim();
  if (!permitId) return;

  const q = `
    SELECT holder_id AS eier, valid_from AS fra, COALESCE(valid_to, 'NÅ') AS til
    FROM ownership_intervals
    WHERE permit_id = $permit
    ORDER BY valid_from
  `;
  const { cols, rows } = runQuery(q, { $permit: permitId });
  renderTable("permitHistResult", cols, rows);
});

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

// ---- DETALJER: Tillatelse -> hele raden (siste snapshot)
document.getElementById("permitDetailsBtn").addEventListener("click", () => {
  const permitId = document.getElementById("permitNowInput").value.trim();
  if (!permitId) return;

  const q = `
    SELECT row_json
    FROM permit_snapshot
    WHERE permit_id = $permit
    ORDER BY snapshot_date DESC
    LIMIT 1
  `;

  const { rows } = runQuery(q, { $permit: permitId });

  if (!rows || rows.length === 0) {
    document.getElementById("permitDetailsResult").innerHTML =
      "<p>Fant ingen detaljer for denne tillatelsen.</p>";
    return;
  }

  const rowObj = JSON.parse(rows[0][0]);
  renderKeyValueTable("permitDetailsResult", rowObj);
});
