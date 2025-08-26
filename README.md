## bh-infra-api

API to query the availability of urban services by CEP (Brazilian postal code) in Belo Horizonte.

All datasets come from the city’s open data portal: [Open Data BH](https://dados.pbh.gov.br/dataset/).

Data is ingested and aggregated into SQLite tables and then loaded in-memory for faster nearest-geometry lookups.

### Requirements
- Node.js 18+
- npm

### Install
```bash
npm install
```

### Input data
Place the CSV files inside the `data/` directory (semicolon `;` separated). Expected examples:
- 20250801_trecho_ilum_publica.csv
- 20250801_trecho_meio_fio.csv
- 20250801_trecho_pavimentacao.csv
- 20250801_trecho_rede_agua.csv
- 20250801_trecho_rede_esgoto.csv
- 20250801_trecho_rede_eletrica.csv
- 20250801_trecho_rede_telefonica.csv
- 20250801_coleta_seletiva_porta_porta.csv

### Ingest data (generates infra.db)
This command recreates `infra.db` on every run.
```bash
npm run ingest
```

### Run the API
```bash
node index.js
```
The API listens on `http://localhost:3002`.

### Endpoint
GET `/infra?cep=<8-digit CEP>`

Example:
```bash
curl 'http://localhost:3002/infra?cep=30140071'
```

### Response
```json
{
  "cep": "30140071",
  "logradouro": "...",
  "bairro": "...",
  "latitude": "-19.9",
  "longitude": "-43.9",
  "servicos": {
    "iluminacao": { "disponivel": "Sim|Não|não informado|não encontrado" },
    "meio_fio": { "disponivel": "..." },
    "pavimentacao": {
      "disponivel": "...",
      "tipo": "...",
      "data_apuracao": "YYYY-MM-DD HH:mm:ss|null"
    },
    "rede_agua": { "disponivel": "..." },
    "rede_esgoto": { "disponivel": "..." },
    "rede_eletrica": { "disponivel": "..." },
    "telefone": { "disponivel": "..." },
    "coleta_seletiva": {
      "disponivel": "...",
      "programacao": "...|null",
      "turno": "...|null",
      "distritos": "...|null",
      "cooperativa_responsavel": "...|null"
    }
  }
}
```

Notes:
- The `disponivel` field maps S/N indicators to Portuguese strings: "Sim"/"Não"; empty values become "não informado"; anything else becomes "não encontrado".
- For paving, if a type exists but the indicator is missing, availability is inferred as "Sim".
- For selective waste collection, any "SEM COLETA ..." program maps to "Não"; values like "NÃO SE APLICA" are treated as not applicable.

### Database structure (SQLite)
- `trecho_geom(id_base_trecho PRIMARY KEY, geojson)`
- `trecho_data(id_base_trecho PRIMARY KEY, ind_ip, ind_mf, ind_pav, tp_pav, data_pav, ind_rdagu, ind_rdesg, ind_re, ind_rt, programacao, turno, nome_distrito, cooperativa_responsavel)`

### License
ISC
