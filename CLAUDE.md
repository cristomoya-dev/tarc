# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

TACRC Stack is a semantic search and RAG (Retrieval-Augmented Generation) system over Spanish public procurement tribunal resolutions (TACRC). It ingests PDF documents, chunks and embeds them, and exposes a hybrid search API plus a conversational chat interface.

## Common commands

```bash
# Start all services (first time or after code changes)
docker compose up --build -d

# Rebuild only the API after code changes
docker compose up --build -d api

# View logs
docker compose logs -f api
docker compose logs -f frontend

# Direct DB access
docker compose exec postgres psql -U tacrc tacrc

# Reset everything (destroys data)
docker compose down -v
```

**Ingest pipeline** (runs as a one-off container, not a long-running service):
```bash
# Full pipeline: metadata + PDF text + embeddings
docker compose run --rm ingest /data/json/resoluciones.json

# Individual steps
docker compose run --rm ingest /data/json/resoluciones.json --only-meta    # ~2 min
docker compose run --rm ingest /data/json/resoluciones.json --only-pdf     # hours
docker compose run --rm ingest /data/json/resoluciones.json --only-embed   # ~1-2h, ~€5-10
```

Health check: `curl http://localhost:8000/health`

## Architecture

Three Docker services on `tacrc_net` internal network, exposed via Traefik (HTTPS):

```
frontend (Streamlit :8501) ──httpx──> api (FastAPI :8003) ──asyncpg──> postgres (pgvector :5433)
                                             │
                                        OpenAI API
                                    (embeddings + chat)
```

**`api/main.py`** — All search and chat logic (~400 lines):
- `/buscar` — Hybrid search: semantic (pgvector HNSW on chunks) + trigram (GIN on text fields), merged with Reciprocal Rank Fusion. Two modes: `semantico` (default) and `exacto`.
- `/chat` — RAG: retrieves top-k chunks by vector distance → optional FlashRank reranking → GPT-4o-mini.
- `/resoluciones/{numero}/resumen` — LLM-extracted structured fields from PDF text.
- In-memory LRU cache (500 entries) for query embeddings to avoid redundant OpenAI calls.

**`api/ingest.py`** — 4-step pipeline:
1. Metadata UPSERT from JSON (Chrome extension export)
2. PDF text extraction (pdfplumber → tesseract OCR fallback)
3. Chunking: ~500 tokens, 50-token overlap, prefixed with resolution header
4. Embedding: `text-embedding-3-small` (1536d), batch=20, 4 concurrent workers

**`frontend/app.py`** — Streamlit UI calling the API via `httpx` (timeouts: 30s search, 60s chat, 180s risk analysis).

## Database schema

Key tables in PostgreSQL 16 with pgvector and pg_trgm extensions:

- **`resoluciones`**: One row per resolution. Contains metadata (`numero`, `fecha`, `sentido`, `tipo_recurso`, `ley_impugnada`), full PDF text (`texto_pdf`), and document-level embedding (`vector(1536)`).
- **`chunks`**: One row per text chunk. Foreign key to `resoluciones`. Contains the chunk text and its embedding (`vector(1536)`). Unique on `(resolucion_id, chunk_index)`.
- **`conversaciones` / `mensajes`**: Chat history with trigger-maintained `total_turnos` counter.

Key indexes:
- `HNSW (cosine)` on `chunks.embedding` and `resoluciones.embedding`
- `GIN trigram` on `descripcion`, `tipo_recurso`, `texto_pdf`

Views `v_estado_ingesta` and `v_stats_anio_sentido` track ingestion progress and data distribution.

## Search query design

The semantic `/buscar` query must be structured so PostgreSQL uses the HNSW index. The critical pattern — the innermost subquery must be `ORDER BY embedding <=> :emb LIMIT N` with no GROUP BY, otherwise the planner falls back to a full sequential scan:

```sql
-- CORRECT: HNSW index is used on the inner query
FROM (
    SELECT resolucion_id, embedding <=> :emb AS dist
    FROM chunks WHERE embedding IS NOT NULL
    ORDER BY embedding <=> :emb LIMIT 600  -- triggers HNSW
) c JOIN resoluciones r ON r.id = c.resolucion_id
GROUP BY r.id ORDER BY MIN(dist) LIMIT 200

-- WRONG: GROUP BY before LIMIT defeats HNSW
SELECT r.id, MIN(c.embedding <=> :emb) FROM chunks c
JOIN resoluciones r ... GROUP BY r.id ORDER BY best_dist LIMIT 200
```

For `text_rank`, the `%` operator (not `similarity()` alone) activates the GIN trigram index. The session must set `pg_trgm.similarity_threshold = 0.05` first (set in the endpoint handler alongside `statement_timeout = '28s'`).

The count is computed in the same query using `COUNT(*) OVER ()` (for exact/no-query modes) or a `total_count` CTE (for semantic mode) — never a separate `COUNT(*)` query, as that would re-execute the expensive CTEs.

## Configuration

All configuration is via `.env`. Key variables:

| Variable | Default | Purpose |
|---|---|---|
| `OPENAI_API_KEY` | — | Required for embeddings and chat |
| `EMBED_MODEL` | `text-embedding-3-small` | Embedding model (1536d) |
| `LLM_MODEL` | `gpt-4o-mini` | Chat and extraction model |
| `POSTGRES_*` | tacrc/tacrc | DB credentials |
| `APP_PASSWORD` | — | Frontend login gate |

API port is `8003` (mapped from container's `8000`). DB port is `5433` (mapped from container's `5432`).
