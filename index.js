import fs from "fs";
import csv from "csv-parser";
import fetch from "node-fetch";
import pkg from "terraformer-wkt-parser";
const { wktToGeoJSON } = pkg;
import proj4 from "proj4";
import turf from "turf";

// SIRGAS2000 / UTM zone 23S (EPSG:31983) → WGS84
proj4.defs("EPSG:31983","+proj=utm +zone=23 +south +datum=SIRGAS2000 +units=m +no_defs");

async function cepToCoords(cep) {
  // 1. Get address info
  const via = await fetch(`https://viacep.com.br/ws/${cep}/json/`).then(r => r.json());
  const query = `${via.logradouro}, ${via.localidade}, ${via.uf}, Brasil`;

  // 2. Geocode to lat/lon (Nominatim)
  const geo = await fetch(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=1`, {
    headers: { "User-Agent": "my-app" }
  }).then(r => r.json());

  if (!geo.length) throw new Error("No geocode found");
  return [parseFloat(geo[0].lon), parseFloat(geo[0].lat)];
}

async function findSegmentsForCep(cep) {
  const [lon, lat] = await cepToCoords(cep);
  console.log("lon, lat", lon, lat);
  const cepPoint = turf.point([lon, lat]);
  console.log("cepPoint", cepPoint);

  const matches = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream("data/20250801_trecho_ilum_publica.csv")
      .pipe(csv({ separator: "\t" })) // adjust delimiter if needed
      .on("data", (row) => {
        try {
          const geom = wktToGeoJSON(row.GEOMETRIA);
          // convert from UTM to WGS84
          geom.coordinates = geom.coordinates.map(([x,y]) => proj4("EPSG:31983","EPSG:4326",[x,y]));
          const line = turf.lineString(geom.coordinates);
          const dist = turf.pointToLineDistance(cepPoint, line, { units: "meters" });
          matches.push({ ...row, distance: dist });
        } catch (e) { /* skip bad rows */ }
      })
      .on("end", () => {
        // sort by distance, keep closest N
        matches.sort((a,b) => a.distance - b.distance);
        resolve(matches.slice(0,10));
      })
      .on("error", reject);
  });
}

// Usage
findSegmentsForCep("30516210").then(results => {
  console.log(results);
});