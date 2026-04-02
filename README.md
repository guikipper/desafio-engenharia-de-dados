# SNCR - Pipeline de Dados de Imóveis Rurais

Pipeline completo de extração, armazenamento e exposição de dados do Sistema Nacional de Cadastro Rural (SNCR).

## Como rodar

```bash
docker-compose up --build
```

Isso inicia 3 serviços:

1. **db** — PostgreSQL com schema criado automaticamente
2. **pipeline** — Extrai dados do SNCR e carrega no banco (roda uma vez e encerra)
3. **api** — FastAPI expondo `GET /imovel/{codigo_incra}` na porta 8000

Após o pipeline concluir, a API estará disponível em `http://localhost:8000`.

### Exemplo de consulta

```bash
curl http://localhost:8000/imovel/01001000000
```

```json
{
  "codigo_incra": "01001000000",
  "denominacao": "Estância Mata Grande",
  "area_ha": 982.93,
  "situacao": "Regular",
  "proprietarios": [
    {
      "nome_completo": "Odete Z.",
      "cpf": "***.***.69-**",
      "vinculo": "Proprietário",
      "participacao_pct": 13.34
    }
  ]
}
```

## Arquitetura

```
SNCR (site) ──► pipeline.py ──► PostgreSQL ──► api.py (FastAPI)
                   │                 │
                   │  1. GET /api/estados
                   │  2. GET /api/municipios/{uf}
                   │  3. GET /api/captcha (resolve automaticamente)
                   │  4. GET /api/dados-abertos/exportar (CSV)
                   │  5. GET /api/consulta/imovel/{codigo} (detalhes)
                   │                 │
                   └─ UPSERT ───────┘
```

## Decisões técnicas

### Extração
- **Duas estratégias de download complementares**: a exportação CSV com município selecionado retorna um subconjunto diferente da exportação sem município ("Todos os municípios"). A união de ambas é necessária para obter o dataset completo (2.989 imóveis vs ~500 de cada forma isolada).
- **Captcha trivial**: a API retorna o campo `digits` junto com o `captcha_id`, permitindo resolução programática sem OCR.
- **Captcha descartável**: cada captcha só pode ser usado uma vez, então geramos um novo antes de cada request.
- **Retry com backoff exponencial**: até 5 tentativas por request, com espera crescente.
- **Checkpointing**: a consulta INCRA salva progresso no banco a cada 50 registros, permitindo retomada em caso de interrupção.

### Modelagem do banco
- **Duas tabelas normalizadas**: `imoveis` (1:N) `proprietarios`, ligadas por `codigo_incra`.
- **Idempotência via UPSERT**: `ON CONFLICT DO UPDATE` em ambas as tabelas. O pipeline pode rodar múltiplas vezes sem gerar duplicatas.
- **Tabela de log**: `extracoes_log` registra metadados de cada extração (data/hora, UF, município, total de registros, status).

### Índices
- `imoveis.codigo_incra` — PRIMARY KEY (índice B-tree automático). Garante busca O(log n) por código INCRA.
- `proprietarios.codigo_incra` — índice na FK para acelerar o JOIN na consulta da API.
- `imoveis(uf)` e `imoveis(uf, municipio)` — índices para consultas analíticas por região.

Com ~3.000 imóveis e ~9.000 proprietários, qualquer consulta por código INCRA executa em < 1ms.

### EXPLAIN ANALYZE

```sql
EXPLAIN ANALYZE
SELECT i.codigo_incra, i.area_hectares, i.situacao, i.denominacao,
       p.nome, p.cpf, p.vinculo, p.pct_participacao
FROM imoveis i
LEFT JOIN proprietarios p ON p.codigo_incra = i.codigo_incra
WHERE i.codigo_incra = '01001000000';
```

```
Nested Loop Left Join  (actual time=0.363..0.386 rows=4 loops=1)
  ->  Index Scan using imoveis_pkey on imoveis i  (actual time=0.160..0.181 rows=1)
        Index Cond: (codigo_incra = '01001000000')
  ->  Index Scan using idx_proprietarios_codigo_incra on proprietarios p  (actual time=0.143..0.145 rows=4)
        Index Cond: (codigo_incra = '01001000000')
Planning Time: 9.081 ms
Execution Time: 1.237 ms
```

A busca usa **Index Scan** nas duas tabelas (PK do `imoveis` + índice da FK em `proprietarios`), executando em **~0.4ms** — bem abaixo do SLA de 2 segundos.

### API
- **CPF anonimizado**: expõe apenas os 2 últimos dígitos antes do verificador, conforme especificação.
- **404 com mensagem clara**: quando o código INCRA não existe na base.
- **Consulta ao PostgreSQL**: a API não acessa o site original.

## O que faria diferente com mais tempo
- Adicionar testes automatizados (pytest) para o pipeline e a API
- Usar async (httpx + asyncpg) para paralelizar as requisições de extração
- Adicionar paginação e filtros na API (por UF, município, situação)
- Implementar cache (Redis) para consultas frequentes
- Monitoramento com métricas (Prometheus) e logs estruturados (JSON)
- CI/CD com GitHub Actions para validar o pipeline automaticamente

## Stack

| Camada | Tecnologia |
|--------|-----------|
| Extração | Python + requests |
| Banco | PostgreSQL 16 |
| API | FastAPI + uvicorn |
| Infra | Docker + docker-compose |
