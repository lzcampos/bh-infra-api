import http from "http";
import { URL, fileURLToPath } from "url";
import fs from "fs";
import csv from "csv-parser";
import fetch from "node-fetch";
import { wktToGeoJSON } from "@terraformer/wkt"
import proj4 from "proj4";
import turf from "turf";
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

function transformCoordinatesRecursive(coords) {
  if (typeof coords[0] === "number") {
    const [x, y] = coords;
    return proj4("EPSG:4326","EPSG:31983", [x, y]);
  }
  return coords.map(transformCoordinatesRecursive);
}

function transformGeometryToWGS84(geom) {
  if (!geom || !geom.type || !geom.coordinates) return geom;
  return { ...geom, coordinates: transformCoordinatesRecursive(geom.coordinates) };
}

function booleanPointInAnyPolygon(point, geom) {
  if (!geom) return false;
  if (geom.type === "Polygon") {
    console.log("coordinates", geom.coordinates);
    const poly = { type: "Feature", geometry: { type: "Polygon", coordinates: geom.coordinates } };
    return turf.booleanPointInPolygon(point, poly);
  }
  if (geom.type === "MultiPolygon") {
    for (const polyCoords of geom.coordinates) {
      const poly = { type: "Feature", geometry: { type: "Polygon", coordinates: polyCoords } };
      if (turf.booleanPointInPolygon(point, poly)) return true;
    }
  }
  return false;
}

function pointToGeometryMinDistanceMeters(point, geom) {
  if (!geom) return Infinity;
  if (geom.type === "LineString") {
    const line = turf.lineString(geom.coordinates);
    return turf.pointToLineDistance(point, line, { units: "meters" });
  }
  if (geom.type === "MultiLineString") {
    let min = Infinity;
    for (const lineCoords of geom.coordinates) {
      const line = turf.lineString(lineCoords);
      const d = turf.pointToLineDistance(point, line, { units: "meters" });
      if (d < min) min = d;
    }
    return min;
  }
  // For polygons, compute distance to outer ring as approximation
  if (geom.type === "Polygon") {
    const line = turf.lineString(geom.coordinates[0]);
    return turf.pointToLineDistance(point, line, { units: "meters" });
  }
  return Infinity;
}

function readCsvStreamClosestFeature(filePath, onRowGeom) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(filePath)) return resolve(null);
    let best = null;
    fs.createReadStream(filePath)
      .pipe(csv({ separator: "\t" }))
      .on("data", (row) => {
        try {
          const rawWkt = row.GEOMETRIA || row.Geometria || row.geometry || row.geom;
          if (!rawWkt) return;
          const parsed = wktToGeoJSON(rawWkt);
          const geom = transformGeometryToWGS84(parsed);
          const dataForCompare = onRowGeom(row, geom);
          if (!dataForCompare) return;
          const { distanceMeters } = dataForCompare;
          if (distanceMeters == null) return;
          if (!best || distanceMeters < best.distanceMeters) best = { ...dataForCompare, row, geom };
        } catch (_) {
          // skip malformed row
        }
      })
      .on("end", () => resolve(best))
      .on("error", reject);
  });
}

async function ensureCepInsideKnownRegion(point) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(STREETS_FILE)) return reject(Object.assign(new Error("Base de centralidade ausente"), { status: 500, code: "BASE_CENTRALIDADE_AUSENTE" }));
    let found = false;
    fs.createReadStream(STREETS_FILE)
      .pipe(csv({ separator: ";" }))
      .on("data", (row) => {
        if (found) return; // allow stream to drain, but ignore
        try {
          
          const rawWkt = row['GEOMETRIA'];
          //console.log("rawWkt", rawWkt);
          if (!rawWkt) return;
          const parsed = wktToGeoJSON(rawWkt);
          if (booleanPointInAnyPolygon(point, parsed.geom)) {
            found = true;
            resolve({ ok: true, row });
          }
        } catch (_) {
          console.error("Error", row);
          // ignore row errors
        }
      })
      .on("end", () => {
        if (!found) reject(Object.assign(new Error("Endereço fora da base de dados"), { status: 404, code: "ENDERECO_FORA_DA_BASE" }));
      })
      .on("error", reject);
  });
}

function mapIndicatorToDisponivel(value) {
  const v = (value || "").toString().trim().toUpperCase();
  if (v === "S") return "Sim";
  if (v === "N") return "Não";
  if (v === "") return "não informado";
  return "não informado";
}

async function analyzeServiceNearest(point, serviceKey) {
  const file = FILES[serviceKey];
  if (!file) return null;

  const best = await readCsvStreamClosestFeature(file, (row, geom) => {
    const distanceMeters = pointToGeometryMinDistanceMeters(point, geom);
    return { distanceMeters };
  });

  if (!best) return { disponivel: "não encontrado", tipo: undefined, data_apuracao: null };
  if (best.distanceMeters > DISTANCE_THRESHOLD_METERS) return { disponivel: "não encontrado", tipo: undefined, data_apuracao: null };

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
      const cepPoint = turf.point([lon, lat]);
      const cepPoint31983 = proj4("EPSG:4326",
        "+proj=utm +zone=23 +south +ellps=GRS80 +units=m +no_defs", 
        cepPoint.geometry?.coordinates);
      console.log("cepPoint31983", cepPoint31983);

      console.log("cepPoint", cepPoint);
      await ensureCepInsideKnownRegion(cepPoint31983); // throws if outside

      const [iluminacao, meio_fio, pavimentacao, rede_agua, rede_esgoto, rede_eletrica, telefone] = await Promise.all([
        analyzeServiceNearest(cepPoint, "iluminacao"),
        analyzeServiceNearest(cepPoint, "meio_fio"),
        analyzeServiceNearest(cepPoint, "pavimentacao"),
        analyzeServiceNearest(cepPoint, "rede_agua"),
        analyzeServiceNearest(cepPoint, "rede_esgoto"),
        analyzeServiceNearest(cepPoint, "rede_eletrica"),
        analyzeServiceNearest(cepPoint, "telefone"),
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