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
  { file: "20250801_coleta_seletiva_porta_porta.csv" },
];

// Schema: one row per trecho in trecho_data (id_base_trecho as PK)
// Geometry is kept in a separate table keyed by id_base_trecho as well
const CREATE_SQL = `
CREATE TABLE IF NOT EXISTS trecho_geom (
  id_base_trecho TEXT PRIMARY KEY,
  geojson TEXT
);
CREATE TABLE IF NOT EXISTS trecho_data (
  id_base_trecho TEXT PRIMARY KEY,
  ind_ip TEXT,
  ind_mf TEXT,
  ind_pav TEXT,
  tp_pav TEXT,
  data_pav TEXT,
  ind_rdagu TEXT,
  ind_rdesg TEXT,
  ind_re TEXT,
  ind_rt TEXT,
  programacao TEXT,
  turno TEXT,
  nome_distrito TEXT,
  cooperativa_responsavel TEXT
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

  const lower = file.toLowerCase();
  const isIlum = lower.includes("ilum_publica");
  const isMf = lower.includes("meio_fio");
  const isPav = lower.includes("pavimentacao");
  const isAgua = lower.includes("rede_agua");
  const isEsgoto = lower.includes("rede_esgoto");
  const isEletrica = lower.includes("rede_eletrica");
  const isTelefone = lower.includes("rede_telefonica");
  const isColeta = lower.includes("coleta_seletiva_porta_porta");

  const upsertGeom = db.prepare(`
    INSERT INTO trecho_geom (id_base_trecho, geojson)
    VALUES (?, ?)
    ON CONFLICT(id_base_trecho) DO UPDATE SET geojson=excluded.geojson
  `);

  const upsertIlum = db.prepare(`
    INSERT INTO trecho_data (id_base_trecho, ind_ip)
    VALUES (?, ?)
    ON CONFLICT(id_base_trecho) DO UPDATE SET ind_ip=excluded.ind_ip
  `);
  const upsertMf = db.prepare(`
    INSERT INTO trecho_data (id_base_trecho, ind_mf)
    VALUES (?, ?)
    ON CONFLICT(id_base_trecho) DO UPDATE SET ind_mf=excluded.ind_mf
  `);
  const upsertPav = db.prepare(`
    INSERT INTO trecho_data (id_base_trecho, ind_pav, tp_pav, data_pav)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(id_base_trecho) DO UPDATE SET
      ind_pav = COALESCE(NULLIF(excluded.ind_pav, ''), trecho_data.ind_pav),
      tp_pav = COALESCE(NULLIF(excluded.tp_pav, ''), trecho_data.tp_pav),
      data_pav = CASE
        WHEN COALESCE(NULLIF(excluded.data_pav, ''), '') = '' THEN trecho_data.data_pav
        WHEN trecho_data.data_pav IS NULL OR trecho_data.data_pav < excluded.data_pav THEN excluded.data_pav
        ELSE trecho_data.data_pav
      END
  `);
  const upsertAgua = db.prepare(`
    INSERT INTO trecho_data (id_base_trecho, ind_rdagu)
    VALUES (?, ?)
    ON CONFLICT(id_base_trecho) DO UPDATE SET
      ind_rdagu = COALESCE(NULLIF(excluded.ind_rdagu, ''), trecho_data.ind_rdagu)
  `);
  const upsertEsgoto = db.prepare(`
    INSERT INTO trecho_data (id_base_trecho, ind_rdesg)
    VALUES (?, ?)
    ON CONFLICT(id_base_trecho) DO UPDATE SET
      ind_rdesg = COALESCE(NULLIF(excluded.ind_rdesg, ''), trecho_data.ind_rdesg)
  `);
  const upsertEletrica = db.prepare(`
    INSERT INTO trecho_data (id_base_trecho, ind_re)
    VALUES (?, ?)
    ON CONFLICT(id_base_trecho) DO UPDATE SET ind_re=excluded.ind_re
  `);
  const upsertTelefone = db.prepare(`
    INSERT INTO trecho_data (id_base_trecho, ind_rt)
    VALUES (?, ?)
    ON CONFLICT(id_base_trecho) DO UPDATE SET ind_rt=excluded.ind_rt
  `);
  const upsertColeta = db.prepare(`
    INSERT INTO trecho_data (id_base_trecho, programacao, turno, nome_distrito, cooperativa_responsavel)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(id_base_trecho) DO UPDATE SET
      programacao=excluded.programacao,
      turno=excluded.turno,
      nome_distrito=excluded.nome_distrito,
      cooperativa_responsavel=excluded.cooperativa_responsavel
  `);

  const geomBatch = [];
  const dataBatch = [];
  let processed = 0;
  let skipped = 0;

  const flush = db.transaction(() => {
    for (const [id, geojson] of geomBatch.splice(0)) upsertGeom.run(id, geojson);
    for (const { type, params } of dataBatch.splice(0)) {
      switch (type) {
        case "ilum": upsertIlum.run(...params); break;
        case "mf": upsertMf.run(...params); break;
        case "pav": upsertPav.run(...params); break;
        case "agua": upsertAgua.run(...params); break;
        case "esgoto": upsertEsgoto.run(...params); break;
        case "eletrica": upsertEletrica.run(...params); break;
        case "telefone": upsertTelefone.run(...params); break;
        case "coleta": upsertColeta.run(...params); break;
      }
    }
  });

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv({ separator: ";" }))
      .on("data", (row) => {
        try {
          const id = normalizeKey(row.ID_BASE_TRECHO);
          if (!id) { skipped++; return; }

          // geometry
          const wkt = normalizeKey(row.GEOMETRIA);
          const geo = parseGeometryWktToGeoJSON(wkt);
          if (geo) {
            geomBatch.push([id, JSON.stringify(geo)]);
          }

          if (isIlum) {
            dataBatch.push({ type: "ilum", params: [id, normalizeKey(row.IND_IP)] });
          } else if (isMf) {
            dataBatch.push({ type: "mf", params: [id, normalizeKey(row.IND_MF)] });
          } else if (isPav) {
            dataBatch.push({ type: "pav", params: [
              id,
              normalizeKey(row.IND_PAV),
              normalizeKey(row.TP_PAV),
              normalizeKey(row.DATA),
            ] });
          } else if (isAgua) {
            dataBatch.push({ type: "agua", params: [
              id,
              normalizeKey(row.IND_RDAGU),
            ] });
          } else if (isEsgoto) {
            dataBatch.push({ type: "esgoto", params: [
              id,
              normalizeKey(row.IND_RDESG),
            ] });
          } else if (isEletrica) {
            dataBatch.push({ type: "eletrica", params: [id, normalizeKey(row.IND_RE)] });
          } else if (isTelefone) {
            dataBatch.push({ type: "telefone", params: [id, normalizeKey(row.IND_RT)] });
          } else if (isColeta) {
            dataBatch.push({ type: "coleta", params: [
              id,
              normalizeKey(row.PROGRAMACAO),
              normalizeKey(row.TURNO),
              normalizeKey(row.NOME_DISTRITO),
              normalizeKey(row.COOPERATIVA_RESPONSAVEL),
            ] });
          } else {
            // unknown file type; ignore
          }

          processed++;
          if ((geomBatch.length + dataBatch.length) >= 1000) flush();
        } catch (e) {
          skipped++;
        }
      })
      .on("end", () => {
        if (geomBatch.length || dataBatch.length) flush();
        resolve({ inserted: processed, skipped });
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
    console.log(`ingesting ${f.file}...`);
    const { inserted, skipped } = await ingestFile(db, f);
    totalInserted += inserted;
    totalSkipped += skipped;
    console.log(`done ${f.file}: processed=${inserted} skipped=${skipped}`);
  }

  // Simple metadata table
  db.exec(`CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);`);
  const setMeta = db.prepare(`INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`);
  setMeta.run("generated_at", new Date().toISOString());
  setMeta.run("source", "csv");

  console.log(`All done. Processed=${totalInserted} Skipped=${totalSkipped}. DB: ${DB_PATH}`);
  const cntGeom = db.prepare("SELECT COUNT(*) AS c FROM trecho_geom").get().c;
  const cntData = db.prepare("SELECT COUNT(*) AS c FROM trecho_data").get().c;
  console.log(`Trecho geom: ${cntGeom}`);
  console.log(`Trecho data: ${cntData}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});


