"""
TACRC PDF Downloader
====================
Descarga masiva de PDFs del TACRC a partir del JSON exportado por la extensión Chrome.

Estructura de salida:
    pdfs/
    └── 2026/
        ├── 0340_2026.pdf
        ├── 0482_2026.pdf
        └── ...
    └── 2025/
        └── ...

Uso:
    python download_pdfs.py resoluciones.json
    python download_pdfs.py resoluciones.json --output ./mis_pdfs --workers 5 --delay 0.5
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import time
import argparse
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import httpx
from tqdm import tqdm
from tqdm.asyncio import tqdm as atqdm


# ── Configuración por defecto ────────────────────────────────────────────────

DEFAULT_OUTPUT_DIR = Path("pdfs")
DEFAULT_WORKERS    = 4       # descargas en paralelo
DEFAULT_DELAY      = 0.5     # segundos entre peticiones por worker
DEFAULT_TIMEOUT    = 30      # timeout por petición
DEFAULT_RETRIES    = 3       # reintentos por fichero
DEFAULT_BACKOFF    = 2.0     # multiplicador de espera entre reintentos

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    ),
    "Referer": "https://www.hacienda.gob.es/",
    "Accept": "application/pdf,*/*",
}


# ── Modelos ──────────────────────────────────────────────────────────────────

@dataclass
class Resolution:
    numero:          str
    fecha:           str
    url:             str
    tipo_recurso:    str = ""
    ley_impugnada:   str = ""
    sentido:         str = ""
    descripcion:     str = ""

@dataclass
class DownloadResult:
    numero:   str
    url:      str
    path:     Path | None
    skipped:  bool  = False
    error:    str   = ""

    @property
    def ok(self):
        return self.path is not None and not self.error


# ── Helpers ──────────────────────────────────────────────────────────────────

def extract_year(fecha: str, url: str) -> str:
    """Extrae el año de la fecha de resolución o de la URL."""
    # Fecha formato DD/MM/YYYY
    m = re.search(r"\b(20\d{2})\b", fecha)
    if m:
        return m.group(1)
    # Fallback: buscar en la URL
    m = re.search(r"año\s*(\d{4})", url, re.IGNORECASE)
    if m:
        return m.group(1)
    return "sin_año"


def safe_filename(numero: str) -> str:
    """Convierte '0340/2026' → '0340_2026'."""
    return re.sub(r"[/\\:*?\"<>|]", "_", numero.strip())


def load_json(path: Path) -> list[Resolution]:
    """Carga el JSON exportado por la extensión y devuelve una lista de Resolution."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    resolutions = []
    for item in data:
        url = item.get("pdfUrl", "").strip()
        if not url:
            continue
        resolutions.append(Resolution(
            numero       = item.get("numero", ""),
            fecha        = item.get("fechaResolucion", ""),
            url          = url,
            tipo_recurso = item.get("tipoRecurso", ""),
            ley_impugnada= item.get("leyImpugnada", ""),
            sentido      = item.get("sentido", ""),
            descripcion  = item.get("descripcion", ""),
        ))
    return resolutions


def dest_path(res: Resolution, output_dir: Path) -> Path:
    """Calcula la ruta de destino del PDF."""
    year = extract_year(res.fecha, res.url)
    name = safe_filename(res.numero) + ".pdf"
    return output_dir / year / name


# ── Descarga ─────────────────────────────────────────────────────────────────

async def download_one(
    client:     httpx.AsyncClient,
    res:        Resolution,
    output_dir: Path,
    delay:      float,
    retries:    int,
    backoff:    float,
    semaphore:  asyncio.Semaphore,
) -> DownloadResult:
    """Descarga un PDF con reintentos y skip si ya existe."""
    path = dest_path(res, output_dir)

    # Skip si ya existe y tiene contenido
    if path.exists() and path.stat().st_size > 0:
        return DownloadResult(numero=res.numero, url=res.url, path=path, skipped=True)

    path.parent.mkdir(parents=True, exist_ok=True)

    async with semaphore:
        await asyncio.sleep(delay)

        last_error = ""
        wait = delay

        for attempt in range(1, retries + 1):
            try:
                async with client.stream("GET", res.url, headers=HEADERS) as response:
                    if response.status_code == 404:
                        return DownloadResult(
                            numero=res.numero, url=res.url, path=None,
                            error=f"404 Not Found"
                        )
                    response.raise_for_status()

                    # Verificar que es realmente un PDF
                    ct = response.headers.get("content-type", "")
                    if "html" in ct:
                        return DownloadResult(
                            numero=res.numero, url=res.url, path=None,
                            error="Respuesta HTML (probablemente redirect a login)"
                        )

                    # Escribir en disco en streaming
                    tmp = path.with_suffix(".tmp")
                    with open(tmp, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=65536):
                            f.write(chunk)

                    tmp.rename(path)
                    return DownloadResult(numero=res.numero, url=res.url, path=path)

            except httpx.HTTPStatusError as e:
                last_error = f"HTTP {e.response.status_code}"
            except httpx.TimeoutException:
                last_error = "Timeout"
            except Exception as e:
                last_error = str(e)

            if attempt < retries:
                await asyncio.sleep(wait)
                wait *= backoff

        # Limpiar tmp si quedó a medias
        tmp = path.with_suffix(".tmp")
        if tmp.exists():
            tmp.unlink(missing_ok=True)

        return DownloadResult(
            numero=res.numero, url=res.url, path=None,
            error=f"Fallido tras {retries} intentos: {last_error}"
        )


# ── Pipeline principal ────────────────────────────────────────────────────────

async def run(
    resolutions: list[Resolution],
    output_dir:  Path,
    workers:     int,
    delay:       float,
    timeout:     int,
    retries:     int,
    backoff:     float,
) -> list[DownloadResult]:

    semaphore = asyncio.Semaphore(workers)
    limits    = httpx.Limits(max_connections=workers + 2, max_keepalive_connections=workers)
    transport = httpx.AsyncHTTPTransport(retries=0)  # reintentos los gestiona download_one

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout),
        limits=limits,
        transport=transport,
        follow_redirects=True,
    ) as client:

        tasks = [
            download_one(client, res, output_dir, delay, retries, backoff, semaphore)
            for res in resolutions
        ]

        results = []
        with tqdm(total=len(tasks), desc="Descargando", unit="pdf",
                  dynamic_ncols=True, colour="cyan") as bar:
            for coro in asyncio.as_completed(tasks):
                result = await coro
                results.append(result)

                status = "⏭" if result.skipped else ("✓" if result.ok else "✗")
                bar.set_postfix_str(f"{status} {result.numero}")
                bar.update(1)

    return results


# ── Informe final ─────────────────────────────────────────────────────────────

def print_report(results: list[DownloadResult], output_dir: Path, elapsed: float):
    ok      = [r for r in results if r.ok]
    skipped = [r for r in results if r.skipped]
    errors  = [r for r in results if r.error]

    total_mb = sum(r.path.stat().st_size for r in ok if r.path and r.path.exists()) / 1_048_576

    print("\n" + "─" * 52)
    print(f"  Total resoluciones  : {len(results):>6}")
    print(f"  Descargadas         : {len(ok):>6}   ({total_mb:.1f} MB)")
    print(f"  Saltadas (ya exist.): {len(skipped):>6}")
    print(f"  Errores             : {len(errors):>6}")
    print(f"  Tiempo              : {elapsed:.1f}s")
    print(f"  Destino             : {output_dir.resolve()}")
    print("─" * 52)

    if errors:
        log_path = output_dir / "errores.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"# Errores de descarga — {datetime.now().isoformat()}\n")
            for r in errors:
                f.write(f"{r.numero}\t{r.url}\t{r.error}\n")
        print(f"\n  ⚠  {len(errors)} errores guardados en: {log_path}")

    # Árbol de carpetas por año
    years = sorted({p.parent.name for r in (ok + skipped) if r.path for p in [r.path]})
    if years:
        print(f"\n  Carpetas creadas: {', '.join(years)}")

    print()


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Descarga masiva de PDFs del TACRC desde el JSON de la extensión Chrome.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("json_file",         type=Path, help="Fichero JSON exportado por la extensión")
    p.add_argument("--output", "-o",    type=Path, default=DEFAULT_OUTPUT_DIR,
                   help=f"Carpeta raíz de descarga (default: {DEFAULT_OUTPUT_DIR})")
    p.add_argument("--workers", "-w",   type=int,  default=DEFAULT_WORKERS,
                   help=f"Descargas en paralelo (default: {DEFAULT_WORKERS})")
    p.add_argument("--delay", "-d",     type=float, default=DEFAULT_DELAY,
                   help=f"Pausa entre peticiones por worker en segundos (default: {DEFAULT_DELAY})")
    p.add_argument("--timeout", "-t",   type=int,  default=DEFAULT_TIMEOUT,
                   help=f"Timeout por petición en segundos (default: {DEFAULT_TIMEOUT})")
    p.add_argument("--retries", "-r",   type=int,  default=DEFAULT_RETRIES,
                   help=f"Reintentos por fichero (default: {DEFAULT_RETRIES})")
    p.add_argument("--only-year",       type=str,  default=None,
                   help="Filtrar solo resoluciones de un año (ej: 2026)")
    p.add_argument("--only-sentido",    type=str,  default=None,
                   help="Filtrar por sentido (ej: Estimación)")
    p.add_argument("--dry-run",         action="store_true",
                   help="Mostrar qué se descargaría sin descargar nada")
    return p.parse_args()


def apply_filters(resolutions: list[Resolution], args) -> list[Resolution]:
    filtered = resolutions

    if args.only_year:
        filtered = [
            r for r in filtered
            if extract_year(r.fecha, r.url) == args.only_year
        ]
        print(f"  Filtro año {args.only_year}: {len(filtered)} resoluciones")

    if args.only_sentido:
        term = args.only_sentido.lower()
        filtered = [r for r in filtered if term in r.sentido.lower()]
        print(f"  Filtro sentido '{args.only_sentido}': {len(filtered)} resoluciones")

    return filtered


async def main():
    args = parse_args()

    if not args.json_file.exists():
        print(f"Error: no se encuentra el fichero '{args.json_file}'")
        sys.exit(1)

    print(f"\n  Cargando {args.json_file} …")
    resolutions = load_json(args.json_file)
    print(f"  {len(resolutions)} resoluciones con URL encontradas")

    resolutions = apply_filters(resolutions, args)

    if not resolutions:
        print("  Nada que descargar tras aplicar filtros.")
        sys.exit(0)

    # Calcular cuántas ya existen
    already = sum(1 for r in resolutions if dest_path(r, args.output).exists())
    pending = len(resolutions) - already
    print(f"  Ya descargadas: {already} | Pendientes: {pending}\n")

    if args.dry_run:
        print("  [--dry-run] Ficheros que se descargarían:")
        for r in resolutions:
            p = dest_path(r, args.output)
            estado = "EXISTS" if p.exists() else "PENDING"
            print(f"    [{estado}] {p}")
        return

    args.output.mkdir(parents=True, exist_ok=True)

    t0 = time.monotonic()
    results = await run(
        resolutions = resolutions,
        output_dir  = args.output,
        workers     = args.workers,
        delay       = args.delay,
        timeout     = args.timeout,
        retries     = args.retries,
        backoff     = DEFAULT_BACKOFF,
    )
    elapsed = time.monotonic() - t0

    print_report(results, args.output, elapsed)


if __name__ == "__main__":
    asyncio.run(main())