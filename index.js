import http from "http";
import { URL } from "url";
import fetch from "node-fetch";
import proj4 from "proj4";
import Database from "better-sqlite3";
import Flatbush from "flatbush";

// CRS: SIRGAS2000 / UTM zone 23S (EPSG:31983) → WGS84
proj4.defs("EPSG:31983","+proj=utm +zone=23 +south +datum=SIRGAS2000 +units=m +no_defs");

const DISTANCE_THRESHOLD_METERS = 50;
const DB_PATH = new URL("./infra.db", import.meta.url).pathname;

function sanitizeCep(raw) {
  const digits = String(raw || "").replace(/\D/g, "");
  return digits.length === 8 ? digits : null;
}

async function cepToViaCep(cep) {
  const url = `https://viacep.com.br/ws/${cep}/json/`;
  const data = await fetch(url).then(r => r.json());
  if (data.erro) throw Object.assign(new Error("CEP não encontrado"), { status: 404, code: "CEP_NAO_ENCONTRADO" });
  return data;
}

async function geocodeFromViaCep(via) {
  const query = `${via.logradouro || ""}, ${via.localidade || ""}, ${via.uf || ""}, Brasil`;
  const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=1`;
  const res = await fetch(url, { headers: { "User-Agent": "bh-infra-api" } }).then(r => r.json());
  if (!Array.isArray(res) || !res.length) throw Object.assign(new Error("Geocodificação não encontrada"), { status: 404, code: "GEOCODE_NAO_ENCONTRADO" });
  return [parseFloat(res[0].lon), parseFloat(res[0].lat)];
}

// Distance helpers
function pointToSegmentDistance(px, py, x1, y1, x2, y2) {
  const apx = px - x1;
  const apy = py - y1;
  const abx = x2 - x1;
  const aby = y2 - y1;

  const ab2 = abx * abx + aby * aby;
  if (ab2 === 0) {
    const dx = px - x1;
    const dy = py - y1;
    return { dist: Math.hypot(dx, dy), cx: x1, cy: y1 };
  }

  let t = (apx * abx + apy * aby) / ab2;
  if (t < 0) t = 0; else if (t > 1) t = 1;
  const cx = x1 + t * abx;
  const cy = y1 + t * aby;
  const dx = px - cx;
  const dy = py - cy;
  return { dist: Math.hypot(dx, dy), cx, cy };
}

function pointToLineStringDistance(pointXY, line) {
  let minDist = Infinity;
  for (let i = 0; i < line.length - 1; i++) {
    const [x1, y1] = line[i];
    const [x2, y2] = line[i + 1];
    const { dist } = pointToSegmentDistance(pointXY[0], pointXY[1], x1, y1, x2, y2);
    if (dist < minDist) minDist = dist;
  }
  return minDist;
}

function computeGeometryMinDistance(pointXY, geometry) {
  if (!geometry) return Infinity;
  if (geometry.type === "LineString" && Array.isArray(geometry.coordinates)) {
    const coords = geometry.coordinates.filter(c => Array.isArray(c) && c.length >= 2 && Number.isFinite(c[0]) && Number.isFinite(c[1]));
    if (coords.length < 2) return Infinity;
    return pointToLineStringDistance(pointXY, coords);
  }
  if (geometry.type === "MultiLineString" && Array.isArray(geometry.coordinates)) {
    let best = Infinity;
    for (const ls of geometry.coordinates) {
      const coords = (ls || []).filter(c => Array.isArray(c) && c.length >= 2 && Number.isFinite(c[0]) && Number.isFinite(c[1]));
      if (coords.length < 2) continue;
      const d = pointToLineStringDistance(pointXY, coords);
      if (d < best) best = d;
    }
    return best;
  }
  return Infinity;
}

function mapIndicatorToDisponivel(value) {
  const v = (value || "").toString().trim().toUpperCase();
  if (v === "S" || v === "SIM" || v === "Y" || v === "1" || v === "TRUE") return "Sim";
  if (v === "N" || v === "NAO" || v === "NÃO" || v === "0" || v === "FALSE") return "Não";
  if (v === "") return "não informado";
  return "não encontrado";
}

function isNotApplicable(value) {
  const v = (value || "").toString().trim().toUpperCase();
  return v === "" || v === "NÃO SE APLICA" || v === "NAO SE APLICA" || v === "N/A" || v === "NA";
}

function mapColetaDisponivel(programacao, turno, nome_distrito, cooperativa_responsavel) {
  const prog = (programacao || "").toString().trim().toUpperCase();
  if (prog.includes("SEM COLETA")) return "Não";
  const hasAnyMeaningful = [programacao, turno, nome_distrito, cooperativa_responsavel].some(v => !isNotApplicable(v));
  return hasAnyMeaningful ? "Sim" : "não encontrado";
}

function computeGeometryBBox(geometry) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const consider = (coords) => {
    for (const c of coords) {
      if (!Array.isArray(c) || c.length < 2) continue;
      const x = c[0];
      const y = c[1];
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
      if (x < minX) minX = x;
      if (y < minY) minY = y;
      if (x > maxX) maxX = x;
      if (y > maxY) maxY = y;
    }
  };
  if (!geometry) return null;
  if (geometry.type === "LineString") {
    consider(geometry.coordinates || []);
  } else if (geometry.type === "MultiLineString") {
    for (const ls of geometry.coordinates || []) consider(ls || []);
  } else {
    return null;
  }
  if (!Number.isFinite(minX) || !Number.isFinite(minY) || !Number.isFinite(maxX) || !Number.isFinite(maxY)) return null;
  return [minX, minY, maxX, maxY];
}

function loadTrechoIndex() {
  const db = new Database(DB_PATH, { readonly: true });
  const geomStmt = db.prepare(`SELECT id_base_trecho, geojson FROM trecho_geom WHERE geojson IS NOT NULL`);
  const dataStmt = db.prepare(`SELECT * FROM trecho_data`);

  const dataMap = new Map();
  for (const d of dataStmt.iterate()) {
    dataMap.set(d.id_base_trecho, d);
  }

  const items = [];
  const bboxes = [];
  for (const row of geomStmt.iterate()) {
    let geom;
    try { geom = JSON.parse(row.geojson); } catch (_) { continue; }
    const bbox = computeGeometryBBox(geom);
    if (!bbox) continue;
    const d = dataMap.get(row.id_base_trecho) || {};
    items.push({
      id_base_trecho: row.id_base_trecho,
      geom,
      ind_ip: d.ind_ip,
      ind_mf: d.ind_mf,
      ind_pav: d.ind_pav,
      tp_pav: d.tp_pav,
      data_pav: d.data_pav,
      ind_rdagu: d.ind_rdagu,
      data_rdagu: d.data_rdagu,
      ind_rdesg: d.ind_rdesg,
      data_rdesg: d.data_rdesg,
      ind_re: d.ind_re,
      ind_rt: d.ind_rt,
      programacao: d.programacao,
      turno: d.turno,
      nome_distrito: d.nome_distrito,
      cooperativa_responsavel: d.cooperativa_responsavel,
    });
    bboxes.push(bbox);
  }

  const index = new Flatbush(bboxes.length);
  for (let i = 0; i < bboxes.length; i++) {
    const [minX, minY, maxX, maxY] = bboxes[i];
    index.add(minX, minY, maxX, maxY);
  }
  index.finish();

  try { db.close(); } catch (_) {}
  return { index, items };
}

function findNearestTrecho(trechoData, pointXY, { maxRadius = 2000, targetCount = 256 } = {}) {
  const [x, y] = pointXY;
  const { index, items } = trechoData;
  if (!index || !items || !items.length) return { bestDist: Infinity, bestItem: null };

  let radius = 50;
  const seen = new Set();
  const candidates = [];
  while (radius <= maxRadius && candidates.length < targetCount) {
    const ids = index.search(x - radius, y - radius, x + radius, y + radius);
    for (const id of ids) {
      if (!seen.has(id)) {
        seen.add(id);
        candidates.push(id);
      }
    }
    radius *= 2;
  }

  let bestDist = Infinity;
  let bestItem = null;
  for (const id of candidates) {
    const item = items[id];
    if (!item) continue;
    const d = computeGeometryMinDistance(pointXY, item.geom);
    if (!Number.isFinite(d)) continue;
    if (d < bestDist) {
      bestDist = d;
      bestItem = item;
    }
  }
  return { bestDist, bestItem };
}

function buildSuccessResponse({ cep, via, lon, lat, services }) {
  return {
    cep,
    logradouro: via.logradouro || "",
    bairro: via.bairro || "",
    latitude: String(lat),
    longitude: String(lon),
    servicos: {
      iluminacao: { disponivel: services.iluminacao?.disponivel || "não encontrado" },
      meio_fio: {
        disponivel: services.meio_fio?.disponivel || "não encontrado"
      },
      pavimentacao: {
        disponivel: services.pavimentacao?.disponivel || "não encontrado",
        tipo: services.pavimentacao?.tipo || "não informado",
        data_apuracao: services.pavimentacao?.data_apuracao || null,
      },
      rede_agua: {
        disponivel: services.rede_agua?.disponivel || "não encontrado"
      },
      rede_esgoto: {
        disponivel: services.rede_esgoto?.disponivel || "não encontrado"
      },
      rede_eletrica: {
        disponivel: services.rede_eletrica?.disponivel || "não encontrado"
      },
      telefone: {
        disponivel: services.telefone?.disponivel || "não encontrado"
      },
      coleta_seletiva: {
        disponivel: services.coleta_seletiva?.disponivel || "não encontrado",
        programacao: services.coleta_seletiva?.programacao || null,
        turno: services.coleta_seletiva?.turno || null,
        distritos: services.coleta_seletiva?.distritos || null,
        cooperativa_responsavel: services.coleta_seletiva?.cooperativa_responsavel || null,
      },
    },
  };
}

function sendJson(res, status, payload) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload));
}

const TRECHO_DATA = loadTrechoIndex();

function startServer() {
  const server = http.createServer(async (req, res) => {
    try {
      console.time("sendJson");
      const url = new URL(req.url, "http://localhost");
      if (req.method !== "GET" || url.pathname !== "/infra") {
        sendJson(res, 404, { error: "ROTA_NAO_ENCONTRADA", message: "Rota não encontrada" });
        return;
      }

      const rawCep = url.searchParams.get("cep");
      const cep = sanitizeCep(rawCep);
      if (!cep) {
        sendJson(res, 400, { error: "CEP_INVALIDO", message: "Informe um CEP válido com 8 dígitos" });
        return;
      }

      const via = await cepToViaCep(cep);

      const [lon, lat] = await geocodeFromViaCep(via);
      if (!Number.isFinite(lon) || !Number.isFinite(lat)) throw new Error("Coordenadas inválidas");

      const [x, y] = proj4("EPSG:4326", "EPSG:31983", [lon, lat]);
      if (!Number.isFinite(x) || !Number.isFinite(y)) throw new Error("Falha na transformação de coordenadas");

      const { bestDist, bestItem } = findNearestTrecho(TRECHO_DATA, [x, y]);
      console.log(bestItem, bestDist);
      const noHit = !bestItem || bestDist > DISTANCE_THRESHOLD_METERS;

      const iluminacao = noHit ? { disponivel: "não encontrado" } : { disponivel: mapIndicatorToDisponivel(bestItem.ind_ip) };
      const meio_fio = noHit ? { disponivel: "não encontrado", data_apuracao: null } : { disponivel: mapIndicatorToDisponivel(bestItem.ind_mf)};
      let pavDisponivel = "não encontrado";
      if (!noHit) {
        const raw = mapIndicatorToDisponivel(bestItem.ind_pav);
        if (raw === "Sim" || raw === "Não") {
          pavDisponivel = raw;
        } else if ((bestItem.tp_pav || "").toString().trim() !== "") {
          pavDisponivel = "Sim";
        } else {
          pavDisponivel = raw;
        }
      }
      const pavimentacao = noHit ? { disponivel: "não encontrado", tipo: "não informado", data_apuracao: null } : { disponivel: pavDisponivel, tipo: bestItem.tp_pav || "não informado", data_apuracao: bestItem.data_pav || null };
      const rede_agua = noHit ? { disponivel: "não encontrado" } : { disponivel: mapIndicatorToDisponivel(bestItem.ind_rdagu) };
      const rede_esgoto = noHit ? { disponivel: "não encontrado" } : { disponivel: mapIndicatorToDisponivel(bestItem.ind_rdesg) };
      const rede_eletrica = noHit ? { disponivel: "não encontrado", data_apuracao: null } : { disponivel: mapIndicatorToDisponivel(bestItem.ind_re), data_apuracao: null };
      const telefone = noHit ? { disponivel: "não encontrado", data_apuracao: null } : { disponivel: mapIndicatorToDisponivel(bestItem.ind_rt), data_apuracao: null };
      const coleta_seletiva = noHit ? {
        disponivel: "não encontrado",
        programacao: null,
        turno: null,
        distritos: null,
        cooperativa_responsavel: null,
      } : {
        disponivel: mapColetaDisponivel(bestItem.programacao, bestItem.turno, bestItem.nome_distrito, bestItem.cooperativa_responsavel),
        programacao: bestItem.programacao || null,
        turno: bestItem.turno || null,
        distritos: bestItem.nome_distrito || null,
        cooperativa_responsavel: bestItem.cooperativa_responsavel || null,
      };

      const payload = buildSuccessResponse({ cep, via, lon, lat, services: { iluminacao, meio_fio, pavimentacao, rede_agua, rede_esgoto, rede_eletrica, telefone, coleta_seletiva } });
      sendJson(res, 200, payload);
    } catch (err) {
      const status = err?.status || 500;
      const code = err?.code || "ERRO_INTERNO";
      sendJson(res, status, { error: code, message: err?.message || "Erro interno" });
    }
  });

  const PORT = process.env.PORT ? Number(process.env.PORT) : 3002;
  server.listen(PORT, () => {
    console.log(`bh-infra-api listening on port ${PORT}`);
  });
  return server;
}

startServer();


