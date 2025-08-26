## bh-infra-api

API para consultar disponibilidade de serviços urbanos por CEP em Belo Horizonte.

Todos os dados foram coletados a partir dos dados abertos de Belo Horizonte(https://dados.pbh.gov.br/dataset/)

Os dados são ingeridos e agregados em tabelas SQLite, e, posteriormente, carregados em memória para maior performance ao calcular distâncias a partir de coordenadas.

### Requisitos
- Node.js 18+
- npm

### Instalação
```bash
npm install
```

### Dados de entrada
Coloque os arquivos CSV dentro do diretório `data/` (separador `;`). Exemplos esperados:
- 20250801_trecho_ilum_publica.csv
- 20250801_trecho_meio_fio.csv
- 20250801_trecho_pavimentacao.csv
- 20250801_trecho_rede_agua.csv
- 20250801_trecho_rede_esgoto.csv
- 20250801_trecho_rede_eletrica.csv
- 20250801_trecho_rede_telefonica.csv
- 20250801_coleta_seletiva_porta_porta.csv

### Ingerindo os dados (gera infra.db)
Este comando recria o banco `infra.db` a cada execução.
```bash
npm run ingest
```

### Executando a API
```bash
node run start
```
A API escuta em `http://localhost:3002`.

### Endpoint
GET `/infra?cep=<CEP8DIGITOS>`

Exemplo:
```bash
curl 'http://localhost:3002/infra?cep=30140071'
```

### Resposta
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

Notas:
- `disponivel` mapeia indicadores S/N para "Sim"/"Não"; valores vazios viram "não informado"; outros viram "não encontrado".
- Para pavimentação, se o tipo existir mas o indicador estiver ausente, inferimos "Sim".
- Para coleta seletiva, "SEM COLETA ..." mapeia para "Não"; valores "NÃO SE APLICA" são tratados como não aplicáveis.

### Estrutura do banco (SQLite)
- `trecho_geom(id_base_trecho PRIMARY KEY, geojson)`
- `trecho_data(id_base_trecho PRIMARY KEY, ind_ip, ind_mf, ind_pav, tp_pav, data_pav, ind_rdagu, ind_rdesg, ind_re, ind_rt, programacao, turno, nome_distrito, cooperativa_responsavel)`

### Licença
ISC
