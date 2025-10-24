# Siglo Fundos — Pipeline e Dashboard

Este repositorio contem um pipeline mensal para ingestao de dados publicos da CVM e fontes complementares (B3 e fallback do portal Mais Retorno) e publica um dashboard estatico hospedado no GitHub Pages. Nao ha custo recorrente de hospedagem: os dados e a interface ficam no proprio GitHub.

## Visao geral

- `data_pipeline/`: codigo Python que baixa, normaliza e consolida as tabelas de fundos.
- `output/`: resultados gerados (CSV, JSON para o dashboard e site estatico).
- `web/`: interface web (HTML, CSS e JavaScript) que consome os JSONs publicados.
- `.github/workflows/monthly_ingest.yml`: workflow mensal que reexecuta o pipeline, atualiza os dados e publica o site.

## Como rodar localmente

1. Criar ambiente:
   ```bash
   python -m venv .venv
   .\.venv\Scripts\activate  # Windows PowerShell
   pip install -r requirements.txt
   ```
2. Configurar:
   ```bash
   copy config\pipeline.yaml config\pipeline.local.yaml
   copy .env.example .env  # crie o arquivo se ainda nao existir
   ```
   Ajuste `config\pipeline.local.yaml` com os CNPJs dos fundos monitorados e, se possuir credenciais do BigQuery, preencha variaveis no `.env`.
   - `meses_retroativos`: quantidade de meses de historico a coletar (ex.: 60 para cerca de 5 anos).
   - `meses_ignorar_recente`: meses mais recentes ignorados para evitar lacunas (padrao 3).
3. Executar apenas com CSV/JSON locais:
   ```bash
   python -m data_pipeline.run_pipeline export-local --config-path config\pipeline.local.yaml
   ```
   Os resultados ficam em `output/staging`, `output/curated`, `output/api` (JSON) e `output/site` (dashboard pronto).
4. Para rodar tudo (incluindo upload ao BigQuery) basta omitir `--skip-bigquery`:
   ```bash
   python -m data_pipeline.run_pipeline ingest --config-path config\pipeline.local.yaml
   ```

## Dados gerados

Após cada execucao sao produzidos:

- `output/staging/*.csv`: fatos e dimensoes brutos.
- `output/curated/*.csv`: agregacoes para analises.
- `output/api/index.json`: indice dos fundos disponibilizado ao front-end.
- `output/api/funds/<cnpj>.json`: series temporais (cota diaria, cotistas, carteira).
- `output/site/`: copia do conteudo de `web/` com subpasta `data/` contendo os JSONs; basta publicar esse diretorio em qualquer hosting estatico.
- Quando os arquivos `INF_MENSAL` nao estiverem disponiveis, o pipeline busca automaticamente as mesmas informacoes nos datasets `FI/DOC/CDA` (composicao de carteira) e `FI/DOC/PERFIL_MENSAL` (quebra de cotistas) da CVM, garantindo que o front receba posicoes e numeros de cotistas atualizados.

## Dashboard estatico

O site usa Chart.js (via CDN) para exibir graficos interativos:

- `valor-cota-chart`: valor da cota e retorno diario.
- `cotistas-chart`: numero de cotistas e patrimonio mensal.
- `holdings-timeline-chart`: historico mensal de participacao por tipo de ativo.
- `holdings-chart`: ultima composicao disponivel e tabela com os principais ativos.

Para testar localmente depois de executar o pipeline:
```bash
python -m http.server --directory output/site 8000
```
Abra `http://localhost:8000` no navegador.

## Automacao no GitHub

O workflow `monthly_ingest.yml`:

1. Roda todo dia 1 do mes (05:00 UTC) ou manualmente.
2. Instala dependencias, restaura a chave GCP (se configurada) e executa `python -m data_pipeline.run_pipeline ingest --skip-bigquery`.
3. Publica `output/site` no GitHub Pages via `actions/deploy-pages`.
4. Caso deseje manter upload ao BigQuery, garanta que os segredos abaixo estejam definidos:
   - `BIGQUERY_SERVICE_ACCOUNT` (conteudo JSON da chave).
   - `BIGQUERY_PROJECT`
   - `BIGQUERY_DATASET_STAGING`
   - `BIGQUERY_DATASET_CURATED`
   - `GCS_BUCKET` (se usar exportacao para GCS).

Se nao utilizar BigQuery, basta remover o passo `upload-bigquery` do workflow ou manter as variaveis vazias para pular o envio.

## Testes

Execute:
```bash
.\.venv\Scripts\activate
pytest
```
Os testes validam parsing da CVM, normalizacao e serializacao para o front-end.

## Boas praticas

- Rode `black .` e `ruff check .` antes de submeter alteracoes.
- Adicione novos fundos em `config/pipeline.yaml` mantendo CNPJs normalizados (apenas digitos).
- Para distribuir o acesso interno, compartilhe o link do GitHub Pages; nao ha necessidade de instalar programas adicionais.
