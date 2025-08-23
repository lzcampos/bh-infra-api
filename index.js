import http from "http";
import { URL, fileURLToPath } from "url";
import fs from "fs";
import csv from "csv-parser";
import fetch from "node-fetch";
import { wktToGeoJSON } from "@terraformer/wkt"
import proj4 from "proj4";
import turf from "turf";
import pointToLineDistance from "@turf/point-to-line-distance";
import { spawn } from "child_process";

// CRS: SIRGAS2000 / UTM zone 23S (EPSG:31983) → WGS84
proj4.defs("EPSG:31983","+proj=utm +zone=23 +south +datum=SIRGAS2000 +units=m +no_defs");

const DATA_DIR = "data";
const STREETS_FILE = `${DATA_DIR}/20250701_trecho_logradouro.csv`;
const FILES = {
  iluminacao: `${DATA_DIR}/20250801_trecho_ilum_publica.csv`,
  meio_fio: `${DATA_DIR}/20250801_trecho_meio_fio.csv`,
  pavimentacao: `${DATA_DIR}/20250801_trecho_pavimentacao.csv`,
  rede_agua: `${DATA_DIR}/20250801_trecho_rede_agua.csv`,
  rede_eletrica: `${DATA_DIR}/20250801_trecho_rede_eletrica.csv`,
  rede_esgoto: `${DATA_DIR}/20250801_trecho_rede_esgoto.csv`,
  telefone: `${DATA_DIR}/20250801_trecho_rede_telefonica.csv`,
};

const DISTANCE_THRESHOLD_METERS = 50; // consider network "near" the address within this distance

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

// Euclidean distance from point to segment
function pointToSegmentDistance(px, py, x1, y1, x2, y2) {
  const apx = px - x1;
  const apy = py - y1;
  const abx = x2 - x1;
  const aby = y2 - y1;

  const ab2 = abx * abx + aby * aby;
  if (ab2 === 0) {
    // segment is a single point
    const dx = px - x1;
    const dy = py - y1;
    return { dist: Math.hypot(dx, dy), cx: x1, cy: y1 };
  }

  // Projection factor t of P onto AB
  let t = (apx * abx + apy * aby) / ab2;
  if (t < 0) t = 0;
  else if (t > 1) t = 1;

  const cx = x1 + t * abx;
  const cy = y1 + t * aby;

  const dx = px - cx;
  const dy = py - cy;

  return { dist: Math.hypot(dx, dy), cx, cy };
}

// Main function: distance from point to polyline
function pointToLineStringDistance(point, line) {
  let minDist = Infinity;
  let closestPoint = null;

  for (let i = 0; i < line.length - 1; i++) {
    const [x1, y1] = line[i];
    const [x2, y2] = line[i + 1];
    const { dist, cx, cy } = pointToSegmentDistance(point[0], point[1], x1, y1, x2, y2);
    if (dist < minDist) {
      minDist = dist;
      closestPoint = [cx, cy];
    }
  }

  return { distance: minDist, closestPoint };
}


function pointToGeometryMinDistanceMeters(point, geom) {
  console.log("point", point);
  console.log("geom", geom);
  if (!geom) return Infinity;
  
  if (geom.type === "LineString" && geom.coordinates && geom.coordinates.length > 1) {
    console.log("geom.coordinates", geom.coordinates);
    
    // Validate and filter coordinates to ensure they're valid numbers
    const validCoordinates = geom.coordinates.filter(coord => {
      if (!Array.isArray(coord) || coord.length < 2) return false;
      const [x, y] = coord;
      return typeof x === 'number' && typeof y === 'number' && 
             !isNaN(x) && !isNaN(y) && 
             isFinite(x) && isFinite(y);
    });
    
    if (validCoordinates.length < 2) {
      console.log("Not enough valid coordinates for LineString");
      return Infinity;
    }
    
    console.log("validCoordinates", validCoordinates);
    
    try {
      return pointToLineStringDistance(point.geometry.coordinates, validCoordinates);
    } catch (err) {
      console.error("Error calculating distance:", err);
      return Infinity;
    }
  }
  
  return Infinity;
}

function readCsvStreamClosestFeature(filePath, onRowGeom) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(filePath)) return resolve(null);
    let best = null;
    fs.createReadStream(filePath)
      .pipe(csv({ separator: ";" }))
      .on("data", (row) => {
        try {
          const rawWkt = row.GEOMETRIA;
          if (!rawWkt || typeof rawWkt !== 'string' || rawWkt.trim() === '') {
            return;
          }
          
          const parsed = wktToGeoJSON(rawWkt);
          console.log("parsed", parsed);
          
          // Validate parsed geometry has valid structure
          if (!parsed || !parsed.coordinates) {
            console.log("Invalid geometry structure, skipping row");
            return;
          }
          
          const dataForCompare = onRowGeom(row, parsed);
          console.log("dataForCompare", dataForCompare);
          console.log("best", best);
          if (!dataForCompare) return;
          const { distanceMeters } = dataForCompare;
          console.log("distanceMeters", distanceMeters);
          if (distanceMeters == null) return;
          if (!best || distanceMeters.distance < best.distanceMeters.distance) best = { ...dataForCompare, row, parsed };
        } catch (err) {
          console.log("Error processing row:", err.message);
          console.log("Problematic row:", row);
          // Continue processing other rows instead of throwing
        }
      })
      .on("end", () => resolve(best))
      .on("error", reject);
  });
}

function mapIndicatorToDisponivel(value) {
  const v = (value || "").toString().trim().toUpperCase();
  if (v === "S") return "Sim";
  if (v === "N") return "Não";
  if (v === "") return "não informado";
  return "não encontrado";
}

async function analyzeServiceNearest(point, serviceKey) {
  const file = FILES[serviceKey];
  if (!file) return null;

  const best = await readCsvStreamClosestFeature(file, (row, geom) => {
    const distanceMeters = pointToGeometryMinDistanceMeters(point, geom);
    return { distanceMeters };
  }); 

  console.log("best result for service", serviceKey, best);


  if (!best) return { disponivel: "não encontrado", tipo: undefined, data_apuracao: null };
  if (best.distanceMeters.distance > DISTANCE_THRESHOLD_METERS) return { disponivel: "não encontrado", tipo: undefined, data_apuracao: null };

  const r = best.row;

  if (serviceKey === "iluminacao") {
    return { disponivel: mapIndicatorToDisponivel(r.IND_IP), tipo: undefined, data_apuracao: null };
  }
  if (serviceKey === "meio_fio") {
    return { disponivel: mapIndicatorToDisponivel(r.IND_MF), tipo: "não informado", data_apuracao: null };
  }
  if (serviceKey === "pavimentacao") {
    return { disponivel: mapIndicatorToDisponivel(r.IND_PAV), tipo: r.TP_PAV || "não informado", data_apuracao: r.DATA || null };
  }
  if (serviceKey === "rede_agua") {
    return { disponivel: mapIndicatorToDisponivel(r.IND_RDAGU), tipo: undefined, data_apuracao: r.DATA || null };
  }
  if (serviceKey === "rede_esgoto") {
    return { disponivel: mapIndicatorToDisponivel(r.IND_RDESG), tipo: undefined, data_apuracao: r.DATA || null };
  }
  if (serviceKey === "rede_eletrica") {
    return { disponivel: mapIndicatorToDisponivel(r.IND_RE), tipo: undefined, data_apuracao: null };
  }
  if (serviceKey === "telefone") {
    return { disponivel: mapIndicatorToDisponivel(r.IND_RT), tipo: undefined, data_apuracao: null };
  }
  if (serviceKey === "coleta_seletiva") {
    return { disponivel: mapIndicatorToDisponivel(r.IND_RT), programacao: null, turno: null, distritos: null, cooperativa_responsavel: null };
  }
  return { disponivel: "não encontrado", tipo: undefined, data_apuracao: null };
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
      console.log("via", via);
      const [lon, lat] = await geocodeFromViaCep(via);
      console.log("lon", lon);
      console.log("lat", lat);
      
      // Validate coordinates before transformation
      if (typeof lon !== 'number' || typeof lat !== 'number' || 
          isNaN(lon) || isNaN(lat) || !isFinite(lon) || !isFinite(lat)) {
        throw new Error("Coordenadas inválidas obtidas da geocodificação");
      }
      
      const transformedCoords = proj4("EPSG:4326", "EPSG:31983", [lon, lat]);
      
      // Validate transformed coordinates
      if (!Array.isArray(transformedCoords) || transformedCoords.length !== 2 ||
          typeof transformedCoords[0] !== 'number' || typeof transformedCoords[1] !== 'number' ||
          isNaN(transformedCoords[0]) || isNaN(transformedCoords[1]) ||
          !isFinite(transformedCoords[0]) || !isFinite(transformedCoords[1])) {
        throw new Error("Falha na transformação de coordenadas");
      }
      
      const cepPoint = turf.point(transformedCoords);
      console.log("cepPoint", cepPoint);

      const [iluminacao, meio_fio, pavimentacao, rede_agua, rede_esgoto, rede_eletrica, telefone] = await Promise.all([
        analyzeServiceNearest(cepPoint, "iluminacao"),
        analyzeServiceNearest(cepPoint, "meio_fio"),
        analyzeServiceNearest(cepPoint, "pavimentacao"),
        analyzeServiceNearest(cepPoint, "rede_agua"),
        analyzeServiceNearest(cepPoint, "rede_esgoto"),
        analyzeServiceNearest(cepPoint, "rede_eletrica"),
        analyzeServiceNearest(cepPoint, "telefone"),
        analyzeServiceNearest(cepPoint, "coleta_seletiva"),
      ]);

      const payload = buildSuccessResponse({ cep, via, lon, lat, services: { iluminacao, meio_fio, pavimentacao, rede_agua, rede_esgoto, rede_eletrica, telefone } });
      sendJson(res, 200, payload);
    } catch (err) {
      const status = err?.status || 500;
      const code = err?.code || "ERRO_INTERNO";
      sendJson(res, status, { error: code, message: err?.message || "Erro interno" });
    }
  });

  const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
  server.listen(PORT, () => {
    // eslint-disable-next-line no-console
    console.log(`bh-infra-api listening on port ${PORT}`);
  });
  return server;
}

const DEV_MODE = process.argv.includes("--dev") || process.argv.includes("--watch") || process.env.DEV_WATCH === "1";
const IS_WATCH_CHILD = process.env._WATCH_CHILD === "1";

function startDevWatcher() {
  const filePath = fileURLToPath(import.meta.url);
  let child = null;

  const spawnChild = () => {
    child = spawn(process.execPath, [filePath], {
      stdio: "inherit",
      env: { ...process.env, _WATCH_CHILD: "1" },
    });
  };

  let debounceTimer = null;
  const restart = () => {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      if (child) {
        try { child.kill(); } catch (_) {}
      }
      spawnChild();
    }, 150);
  };

  const watcher = fs.watch(filePath, { persistent: true }, () => restart());

  process.on("SIGINT", () => {
    try { watcher.close(); } catch (_) {}
    if (child) {
      try { child.kill(); } catch (_) {}
    }
    process.exit(0);
  });

  process.on("SIGTERM", () => {
    try { watcher.close(); } catch (_) {}
    if (child) {
      try { child.kill(); } catch (_) {}
    }
    process.exit(0);
  });

  spawnChild();
}

if (DEV_MODE && !IS_WATCH_CHILD) {
  startDevWatcher();
} else {
  startServer();
}