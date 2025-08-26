import fs from "fs";
import path from "path";
import csv from "csv-parser";
import Database from "better-sqlite3";
import { wktToGeoJSON } from "@terraformer/wkt";

const DATA_DIR = path.resolve("data");
const DB_PATH = path.resolve("infra.db");

// Input files
const FILES = [
  { file: "20250801_trecho_ilum_publica.csv" },
  { file: "20250801_trecho_meio_fio.csv" },
  { file: "20250801_trecho_pavimentacao.csv" },
  { file: "20250801_trecho_rede_agua.csv" },
  { file: "20250801_trecho_rede_eletrica.csv" },
  { file: "20250801_trecho_rede_esgoto.csv" },
  { file: "20250801_trecho_rede_telefonica.csv" },
];

// Single superset schema (all nullable except 'source_file')
// We store geometry as raw WKT and also as GeoJSON string for convenience
const CREATE_SQL = `
CREATE TABLE IF NOT EXISTS infra_features (
  id INTEGER PRIMARY KEY,
  source_file TEXT NOT NULL,
  id_base_trecho TEXT,
  id_base_ip TEXT,
  ind_ip TEXT,
  id_base_mf TEXT,
  ind_mf TEXT,
  id_pav TEXT,
  larg_inicio TEXT,
  larg_final TEXT,
  ind_pav TEXT,
  lado_pav TEXT,
  tp_pav TEXT,
  data TEXT,
  id_rdagu TEXT,
  lado_rdagu TEXT,
  ind_rdagu TEXT,
  id_base_re TEXT,
  ind_re TEXT,
  id_rdesg TEXT,
  lado_rdesg TEXT,
  ind_rdesg TEXT,
  id_base_rt TEXT,
  ind_rt TEXT,
  wkt TEXT,
  geojson TEXT
);
CREATE INDEX IF NOT EXISTS idx_infra_trecho ON infra_features(id_base_trecho);
CREATE INDEX IF NOT EXISTS idx_infra_ind_ip ON infra_features(ind_ip);
CREATE INDEX IF NOT EXISTS idx_infra_ind_mf ON infra_features(ind_mf);
CREATE INDEX IF NOT EXISTS idx_infra_ind_pav ON infra_features(ind_pav);
CREATE INDEX IF NOT EXISTS idx_infra_ind_rdagu ON infra_features(ind_rdagu);
CREATE INDEX IF NOT EXISTS idx_infra_ind_re ON infra_features(ind_re);
CREATE INDEX IF NOT EXISTS idx_infra_ind_rdesg ON infra_features(ind_rdesg);
CREATE INDEX IF NOT EXISTS idx_infra_ind_rt ON infra_features(ind_rt);
-- Geometry-only index table (for R-tree build)
CREATE TABLE IF NOT EXISTS trecho_geom (
  id_base_trecho TEXT PRIMARY KEY,
  geojson TEXT
);
-- Aggregated data table (one row per trecho with all indicators)
CREATE TABLE IF NOT EXISTS trecho_data (
  id_base_trecho TEXT PRIMARY KEY,
  ind_ip TEXT,
  ind_mf TEXT,
  ind_pav TEXT,
  tp_pav TEXT,
  data_pav TEXT,
  ind_rdagu TEXT,
  data_rdagu TEXT,
  ind_rdesg TEXT,
  data_rdesg TEXT,
  ind_re TEXT,
  ind_rt TEXT
);
`;

function normalizeKey(key) {
  return String(key || "").trim();
}

function parseGeometryWktToGeoJSON(wkt) {
  try {
    if (!wkt || typeof wkt !== "string" || wkt.trim() === "") return null;
    const geo = wktToGeoJSON(wkt);
    if (!geo) return null;
    // store only when coordinates exist
    if (Array.isArray(geo.coordinates)) return geo;
    return null;
  } catch (_) {
    return null;
  }
}

async function ingestFile(db, { file }) {
  const filePath = path.join(DATA_DIR, file);
  if (!fs.existsSync(filePath)) {
    console.warn(`skip: ${file} not found`);
    return { inserted: 0, skipped: 0 };
  }

  const insert = db.prepare(`
    INSERT INTO infra_features (
      source_file,
      id_base_trecho, id_base_ip, ind_ip,
      id_base_mf, ind_mf,
      id_pav, larg_inicio, larg_final, ind_pav, lado_pav, tp_pav, data,
      id_rdagu, lado_rdagu, ind_rdagu,
      id_base_re, ind_re,
      id_rdesg, lado_rdesg, ind_rdesg,
      id_base_rt, ind_rt,
      wkt, geojson
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const trx = db.transaction((rows) => {
    for (const r of rows) insert.run(r);
  });

  return new Promise((resolve, reject) => {
    const rowsBuffer = [];
    let inserted = 0;
    let skipped = 0;

    fs.createReadStream(filePath)
      .pipe(csv({ separator: ";" }))
      .on("data", (row) => {
        try {
          const wkt = normalizeKey(row.GEOMETRIA);
          const geo = parseGeometryWktToGeoJSON(wkt);
          if (!geo) {
            skipped++;
            return;
          }

          const payload = [
            file,
            normalizeKey(row.ID_BASE_TRECHO),
            normalizeKey(row.ID_BASE_IP),
            normalizeKey(row.IND_IP),
            normalizeKey(row.ID_BASE_MF),
            normalizeKey(row.IND_MF),
            normalizeKey(row.ID_PAV),
            normalizeKey(row.LARG_INICIO),
            normalizeKey(row.LARG_FINAL),
            normalizeKey(row.IND_PAV),
            normalizeKey(row.LADO_PAV),
            normalizeKey(row.TP_PAV),
            normalizeKey(row.DATA),
            normalizeKey(row.ID_RDAGU),
            normalizeKey(row.LADO_RDAGU),
            normalizeKey(row.IND_RDAGU),
            normalizeKey(row.ID_BASE_RE),
            normalizeKey(row.IND_RE),
            normalizeKey(row.ID_RDESG),
            normalizeKey(row.LADO_RDESG),
            normalizeKey(row.IND_RDESG),
            normalizeKey(row.ID_BASE_RT),
            normalizeKey(row.IND_RT),
            wkt,
            JSON.stringify(geo),
          ];

          rowsBuffer.push(payload);
          if (rowsBuffer.length >= 1000) {
            trx(rowsBuffer.splice(0));
            inserted += 1000;
          }
        } catch (e) {
          skipped++;
        }
      })
      .on("end", () => {
        if (rowsBuffer.length) {
          trx(rowsBuffer);
          inserted += rowsBuffer.length;
        }
        resolve({ inserted, skipped });
      })
      .on("error", reject);
  });
}

async function main() {
  if (!fs.existsSync(DATA_DIR)) {
    console.error(`data directory not found: ${DATA_DIR}`);
    process.exit(1);
  }

  // Recreate DB each run to keep it in sync with CSVs
  if (fs.existsSync(DB_PATH)) fs.rmSync(DB_PATH);

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");

  db.exec(CREATE_SQL);

  let totalInserted = 0;
  let totalSkipped = 0;
  for (const f of FILES) {
    // eslint-disable-next-line no-console
    console.log(`ingesting ${f.file}...`);
    const { inserted, skipped } = await ingestFile(db, f);
    totalInserted += inserted;
    totalSkipped += skipped;
    // eslint-disable-next-line no-console
    console.log(`done ${f.file}: inserted=${inserted} skipped=${skipped}`);
  }

  // Build aggregated tables from infra_features
  // Reset current snapshot
  db.exec(`DELETE FROM trecho_geom;`);
  db.exec(`DELETE FROM trecho_data;`);

  // Geometry: pick one representative non-empty geometry per trecho
  db.exec(`
    INSERT INTO trecho_geom (id_base_trecho, geojson)
    SELECT id_base_trecho,
           MAX(CASE WHEN geojson IS NOT NULL AND geojson != '' THEN geojson END) AS geojson
    FROM infra_features
    WHERE id_base_trecho IS NOT NULL AND id_base_trecho != ''
    GROUP BY id_base_trecho;
  `);

  // Data: aggregate first non-empty indicator per category
  db.exec(`
    INSERT INTO trecho_data (
      id_base_trecho,
      ind_ip,
      ind_mf,
      ind_pav, tp_pav, data_pav,
      ind_rdagu, data_rdagu,
      ind_rdesg, data_rdesg,
      ind_re,
      ind_rt
    )
    SELECT
      id_base_trecho,
      MAX(CASE WHEN ind_ip IS NOT NULL AND ind_ip != '' THEN ind_ip END) AS ind_ip,
      MAX(CASE WHEN ind_mf IS NOT NULL AND ind_mf != '' THEN ind_mf END) AS ind_mf,
      MAX(CASE WHEN ind_pav IS NOT NULL AND ind_pav != '' THEN ind_pav END) AS ind_pav,
      MAX(CASE WHEN ind_pav IS NOT NULL AND ind_pav != '' THEN tp_pav END) AS tp_pav,
      MAX(CASE WHEN ind_pav IS NOT NULL AND ind_pav != '' THEN data END) AS data_pav,
      MAX(CASE WHEN ind_rdagu IS NOT NULL AND ind_rdagu != '' THEN ind_rdagu END) AS ind_rdagu,
      MAX(CASE WHEN ind_rdagu IS NOT NULL AND ind_rdagu != '' THEN data END) AS data_rdagu,
      MAX(CASE WHEN ind_rdesg IS NOT NULL AND ind_rdesg != '' THEN ind_rdesg END) AS ind_rdesg,
      MAX(CASE WHEN ind_rdesg IS NOT NULL AND ind_rdesg != '' THEN data END) AS data_rdesg,
      MAX(CASE WHEN ind_re IS NOT NULL AND ind_re != '' THEN ind_re END) AS ind_re,
      MAX(CASE WHEN ind_rt IS NOT NULL AND ind_rt != '' THEN ind_rt END) AS ind_rt
    FROM infra_features
    WHERE id_base_trecho IS NOT NULL AND id_base_trecho != ''
    GROUP BY id_base_trecho;
  `);

  // Simple metadata table
  db.exec(`CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);`);
  const setMeta = db.prepare(`INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`);
  setMeta.run("generated_at", new Date().toISOString());
  setMeta.run("source", "csv");

  // eslint-disable-next-line no-console
  console.log(`All done. Inserted=${totalInserted} Skipped=${totalSkipped}. DB: ${DB_PATH}`);
  console.log(`Trecho geom: ${db.prepare("SELECT COUNT(*) FROM trecho_geom").get().length}`);
  console.log(`Trecho data: ${db.prepare("SELECT COUNT(*) FROM trecho_data").get().length}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});


