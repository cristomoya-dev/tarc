# TACRC Stack — Guía de puesta en marcha

## Estructura del proyecto

```
tacrc-stack/
├── docker-compose.yml
├── .env.example          ← copia como .env y rellena
├── pdfs/                 ← PDFs descargados (generado por download_pdfs.py)
├── data/                 ← JSON exportado por la extensión Chrome
├── postgres/
│   └── init/
│       └── 00_init.sql   ← esquema, extensiones, índices (automático)
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config.py
│   ├── db.py
│   ├── main.py           ← FastAPI: búsqueda + chat RAG
│   └── ingest.py         ← pipeline de ingesta
└── frontend/
    ├── Dockerfile
    ├── requirements.txt
    └── app.py            ← Streamlit UI
```

## Requisitos

- Docker 24+ y Docker Compose v2
- Clave de API de OpenAI

---

## 1. Configuración inicial

```bash
# Clonar/copiar el proyecto
cd tacrc-stack

# Crear el fichero de entorno
cp .env.example .env

# Editar .env con tus valores:
#   POSTGRES_PASSWORD=...
#   OPENAI_API_KEY=sk-...
#   PDF_HOST_DIR=./pdfs        ← ruta a la carpeta de PDFs en tu máquina
#   JSON_HOST_DIR=./data
nano .env

# Crear carpetas de datos
mkdir -p pdfs data
```

---

## 2. Colocar los datos

```bash
# Copia el JSON exportado por la extensión Chrome
cp ~/Downloads/TACRC_resoluciones_2026-03-27.json data/resoluciones.json

# Los PDFs se descargan con el script separado (ya en otra carpeta)
# Si usaste download_pdfs.py con --output ./pdfs, ya está en su sitio.
```

---

## 3. Arrancar los servicios

```bash
# Primera vez: construir imágenes y levantar
docker compose up --build -d

# Ver logs en tiempo real
docker compose logs -f

# Verificar que todo está sano
docker compose ps
curl http://localhost:8000/health    # → {"status":"ok"}
```

---

## 4. Ingesta de datos

La ingesta tiene tres pasos que puedes lanzar juntos o por separado:

```bash
# Todo de una vez (metadatos + texto PDFs + embeddings)
docker compose run --rm ingest /data/json/resoluciones.json

# O por pasos:
docker compose run --rm ingest /data/json/resoluciones.json --only-meta   # ~2 min para 19k
docker compose run --rm ingest /data/json/resoluciones.json --only-pdf    # horas (depende de PDFs)
docker compose run --rm ingest /data/json/resoluciones.json --only-embed  # ~1-2h (coste ~5-10€)

# Ver estado de la ingesta en cualquier momento
curl http://localhost:8000/stats
```

### Coste estimado de embeddings (OpenAI)

| Modelo | 19.000 docs × ~1.500 tokens | Precio aprox. |
|---|---|---|
| text-embedding-3-small | ~28M tokens | ~5 € |
| text-embedding-3-large | ~28M tokens | ~35 € |

Se recomienda `text-embedding-3-small` para empezar.

---

## 5. Usar la aplicación

| Servicio | URL |
|---|---|
| Frontend (Streamlit) | http://localhost:8501 |
| API (FastAPI docs) | http://localhost:8000/docs |
| PostgreSQL | localhost:5432 (solo desde host) |

---

## 6. Comandos útiles

```bash
# Parar todos los servicios
docker compose down

# Parar Y borrar la base de datos (destructivo)
docker compose down -v

# Reconstruir solo la API tras cambios de código
docker compose up --build -d api

# Conectar directamente a PostgreSQL
docker compose exec postgres psql -U tacrc tacrc

# Ver estadísticas de ingesta
docker compose exec postgres psql -U tacrc tacrc -c "SELECT * FROM v_estado_ingesta;"
docker compose exec postgres psql -U tacrc tacrc -c "SELECT * FROM v_stats_anio_sentido LIMIT 20;"

# Actualizar resoluciones (nuevo JSON de la extensión)
docker compose run --rm ingest /data/json/resoluciones_nuevo.json --only-meta

# Forzar re-ingesta de embeddings (si cambias de modelo)
docker compose exec postgres psql -U tacrc tacrc -c "UPDATE resoluciones SET embedding = NULL, embedding_at = NULL;"
docker compose run --rm ingest /data/json/resoluciones.json --only-embed
```

---

## 7. API — Endpoints principales

```
GET  /health                    Estado del servicio
GET  /stats                     Estadísticas de ingesta y distribución
GET  /filtros                   Valores únicos para los selectores del frontend
GET  /buscar?q=...&anio=2026    Búsqueda híbrida (SQL + vectorial)
GET  /resoluciones/0340_2026    Detalle de una resolución
POST /chat                      Chat RAG en lenguaje natural
```

### Ejemplo de búsqueda

```bash
# Búsqueda semántica + filtro por sentido
curl "http://localhost:8000/buscar?q=solvencia+económica+informe+auditor&sentido=Desestimación&page_size=10"

# Chat RAG
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"pregunta": "¿Cuándo es insuficiente el informe de auditor para acreditar solvencia?", "top_k": 8}'
```
