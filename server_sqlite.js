import http from "http";
import { URL } from "url";
import fetch from "node-fetch";
import proj4 from "proj4";
import Database from "better-sqlite3";

// CRS: SIRGAS2000 / UTM zone 23S (EPSG:31983) → WGS84
proj4.defs("EPSG:31983","+proj=utm +zone=23 +south +datum=SIRGAS2000 +units=m +no_defs");

const DISTANCE_THRESHOLD_METERS = 50;
const DB_PATH = new URL("./infra.db", import.meta.url).pathname;

// DB connection (one per process)
const db = new Database(DB_PATH, { readonly: true });

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

// Distance helpers (same math as in index.js)
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

function mapIndicatorToDisponivel(value) {
  const v = (value || "").toString().trim().toUpperCase();
  if (v === "S") return "Sim";
  if (v === "N") return "Não";
  if (v === "") return "não informado";
  return "não encontrado";
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

function analyzeNearestByIndicator(pointXY, options) {
  const { indicatorColumn, selectExtra = "" } = options;
  const extra = selectExtra ? `, ${selectExtra}` : "";
  const sql = `SELECT geojson, ${indicatorColumn} AS indicator${extra} FROM infra_features WHERE ${indicatorColumn} IS NOT NULL AND ${indicatorColumn} != ''`;
  const stmt = db.prepare(sql);

  let bestDist = Infinity;
  let bestRow = null;

  for (const row of stmt.iterate()) {
    if (!row.geojson) continue;
    let geom;
    try {
      geom = JSON.parse(row.geojson);
    } catch (_) {
      continue;
    }
    const d = computeGeometryMinDistance(pointXY, geom);
    if (!Number.isFinite(d)) continue;
    if (d < bestDist) {
      bestDist = d;
      bestRow = row;
    }
  }

  return { bestDist, bestRow };
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
        disponivel: services.meio_fio?.disponivel || "não encontrado",
        tipo: services.meio_fio?.tipo || "não informado",
        data_apuracao: services.meio_fio?.data_apuracao || null,
      },
      pavimentacao: {
        disponivel: services.pavimentacao?.disponivel || "não encontrado",
        tipo: services.pavimentacao?.tipo || "não informado",
        data_apuracao: services.pavimentacao?.data_apuracao || null,
      },
      rede_agua: {
        disponivel: services.rede_agua?.disponivel || "não encontrado",
        data_apuracao: services.rede_agua?.data_apuracao || null,
      },
      rede_esgoto: {
        disponivel: services.rede_esgoto?.disponivel || "não encontrado",
        data_apuracao: services.rede_esgoto?.data_apuracao || null,
      },
      rede_eletrica: {
        disponivel: services.rede_eletrica?.disponivel || "não encontrado",
        data_apuracao: services.rede_eletrica?.data_apuracao || null,
      },
      telefone: {
        disponivel: services.telefone?.disponivel || "não encontrado",
        data_apuracao: services.telefone?.data_apuracao || null,
      },
      coleta_seletiva: {
        disponivel: "não encontrado",
        programacao: null,
        turno: null,
        distritos: null,
        cooperativa_responsavel: null,
      },
    },
  };
}

function sendJson(res, status, payload) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload));
}

function startServer() {
  const server = http.createServer(async (req, res) => {
    try {
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

      const [
        nearestIlum,
        nearestMeioFio,
        nearestPav,
        nearestAgua,
        nearestEsgoto,
        nearestEletrica,
        nearestTelefone,
      ] = await Promise.all([
        Promise.resolve(analyzeNearestByIndicator([x, y], { indicatorColumn: "ind_ip" })),
        Promise.resolve(analyzeNearestByIndicator([x, y], { indicatorColumn: "ind_mf" })),
        Promise.resolve(analyzeNearestByIndicator([x, y], { indicatorColumn: "ind_pav", selectExtra: "tp_pav, data" })),
        Promise.resolve(analyzeNearestByIndicator([x, y], { indicatorColumn: "ind_rdagu", selectExtra: "data" })),
        Promise.resolve(analyzeNearestByIndicator([x, y], { indicatorColumn: "ind_rdesg", selectExtra: "data" })),
        Promise.resolve(analyzeNearestByIndicator([x, y], { indicatorColumn: "ind_re" })),
        Promise.resolve(analyzeNearestByIndicator([x, y], { indicatorColumn: "ind_rt" })),
      ]);

      const iluminacao = (!nearestIlum.bestRow || nearestIlum.bestDist > DISTANCE_THRESHOLD_METERS)
        ? { disponivel: "não encontrado" }
        : { disponivel: mapIndicatorToDisponivel(nearestIlum.bestRow.indicator) };

      const meio_fio = (!nearestMeioFio.bestRow || nearestMeioFio.bestDist > DISTANCE_THRESHOLD_METERS)
        ? { disponivel: "não encontrado", tipo: "não informado", data_apuracao: null }
        : { disponivel: mapIndicatorToDisponivel(nearestMeioFio.bestRow.indicator), tipo: "não informado", data_apuracao: null };

      const pavimentacao = (!nearestPav.bestRow || nearestPav.bestDist > DISTANCE_THRESHOLD_METERS)
        ? { disponivel: "não encontrado", tipo: "não informado", data_apuracao: null }
        : { disponivel: mapIndicatorToDisponivel(nearestPav.bestRow.indicator), tipo: nearestPav.bestRow.tp_pav || "não informado", data_apuracao: nearestPav.bestRow.data || null };

      const rede_agua = (!nearestAgua.bestRow || nearestAgua.bestDist > DISTANCE_THRESHOLD_METERS)
        ? { disponivel: "não encontrado", data_apuracao: null }
        : { disponivel: mapIndicatorToDisponivel(nearestAgua.bestRow.indicator), data_apuracao: nearestAgua.bestRow.data || null };

      const rede_esgoto = (!nearestEsgoto.bestRow || nearestEsgoto.bestDist > DISTANCE_THRESHOLD_METERS)
        ? { disponivel: "não encontrado", data_apuracao: null }
        : { disponivel: mapIndicatorToDisponivel(nearestEsgoto.bestRow.indicator), data_apuracao: nearestEsgoto.bestRow.data || null };

      const rede_eletrica = (!nearestEletrica.bestRow || nearestEletrica.bestDist > DISTANCE_THRESHOLD_METERS)
        ? { disponivel: "não encontrado", data_apuracao: null }
        : { disponivel: mapIndicatorToDisponivel(nearestEletrica.bestRow.indicator), data_apuracao: null };

      const telefone = (!nearestTelefone.bestRow || nearestTelefone.bestDist > DISTANCE_THRESHOLD_METERS)
        ? { disponivel: "não encontrado", data_apuracao: null }
        : { disponivel: mapIndicatorToDisponivel(nearestTelefone.bestRow.indicator), data_apuracao: null };

      const payload = buildSuccessResponse({ cep, via, lon, lat, services: { iluminacao, meio_fio, pavimentacao, rede_agua, rede_esgoto, rede_eletrica, telefone } });
      sendJson(res, 200, payload);
    } catch (err) {
      const status = err?.status || 500;
      const code = err?.code || "ERRO_INTERNO";
      sendJson(res, status, { error: code, message: err?.message || "Erro interno" });
    }
  });

  const PORT = process.env.PORT ? Number(process.env.PORT) : 3001; // use a different default port
  server.listen(PORT, () => {
    // eslint-disable-next-line no-console
    console.log(`bh-infra-sqlite listening on port ${PORT}`);
  });
  return server;
}

startServer();


