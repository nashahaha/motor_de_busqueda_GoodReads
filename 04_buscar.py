#!/usr/bin/env python3
# =============================================================================
# 04_buscar.py — Motor de búsqueda de libros en Elasticsearch
# Búsqueda multi-campo con boosting y function_score.
#
# Uso:
#   python 04_buscar.py "misterio"
#   python 04_buscar.py "viaje espacial" --lang eng
#   python 04_buscar.py "García Márquez" --min-year 1960 --max-year 2000
#   python 04_buscar.py "superación personal" --min-rating 4.0 --size 20
#   python 04_buscar.py                                   # modo interactivo
#
# Requisito: pip install "elasticsearch>=6,<7"  # cliente compatible con ES 6.8
# =============================================================================

import argparse
import sys
import json
from elasticsearch import Elasticsearch

# --- Argumentos ---------------------------------------------------------------
parser = argparse.ArgumentParser(
    description="Buscar libros en el índice de Goodreads",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""\
Ejemplos:
  python 04_buscar.py "misterio"
  python 04_buscar.py "viaje espacial" --lang eng
  python 04_buscar.py "García Márquez" --min-year 1960 --max-year 2000
  python 04_buscar.py "superación personal" --min-rating 4.0
""",
)
parser.add_argument("query",       nargs="?", default=None, help="Texto a buscar")
# CAMBIO: usar el host del clúster indicado en el Laboratorio 7.
parser.add_argument("--es-host",   default="cm:9200", help="Host:puerto de ES")
# CAMBIO: mantener el índice del grupo consistente con 03_indexar.py.
parser.add_argument("--index",     default="grupo8_goodreads_books", help="Nombre del índice")
parser.add_argument("--lang",      default=None, help="Filtrar por idioma (ej: eng, spa)")
parser.add_argument("--min-year",  type=int, default=None, help="Año mínimo de publicación")
parser.add_argument("--max-year",  type=int, default=None, help="Año máximo de publicación")
parser.add_argument("--min-rating",type=float, default=None, help="Rating mínimo (0-5)")
parser.add_argument("--size",      type=int, default=10, help="Número de resultados")
parser.add_argument("--debug",     action="store_true", help="Mostrar query JSON enviada")
args = parser.parse_args()
# CAMBIO: evitar consultas con tamaño inválido o excesivo en el clúster compartido.
if not 1 <= args.size <= 100:
    parser.error("--size debe estar entre 1 y 100")


def build_query(text, lang=None, min_year=None, max_year=None,
                min_rating=None, size=10):
    """
    Construye la query de Elasticsearch:
    - multi_match con boosts por campo
    - function_score para favorecer libros con mejor Rating y más reseñas
    - filtros opcionales por idioma, año y rating mínimo
    """

    # --- Parte principal: multi_match con boosts ------------------------------
    multi_match = {
        "multi_match": {
            "query":  text,
            "fields": [
                "Description^3",    # Mayor peso si aparece en la descripción
                "Name^2",           # Peso alto en el título
                "Authors^1.5",      # Peso medio en autores
                "Publisher",        # Peso base en editorial
                "searchable_text",  # Red de seguridad: siempre tiene contenido
            ],
            "type": "best_fields",
            "fuzziness": "AUTO",    # Tolerancia a errores tipográficos
        }
    }

    # --- Filtros opcionales ---------------------------------------------------
    filters = []
    if lang:
        # CAMBIO: los códigos keyword se guardan en minúsculas en 02_limpiar.py.
        filters.append({"term": {"Language": lang.strip().lower()}})
    if min_year is not None or max_year is not None:
        year_range = {}
        if min_year is not None:
            year_range["gte"] = min_year
        if max_year is not None:
            year_range["lte"] = max_year
        filters.append({"range": {"PublishYear": year_range}})
    # CAMBIO: 0.0 es un valor válido y no debe interpretarse como ausencia de filtro.
    if min_rating is not None:
        filters.append({"range": {"Rating": {"gte": min_rating}}})

    # --- Combinar multi_match con filtros en bool query -----------------------
    if filters:
        bool_query = {
            "bool": {
                "must":   multi_match,
                "filter": filters,
            }
        }
    else:
        bool_query = multi_match

    # --- function_score: boost por Rating y CountsOfReview --------------------
    query = {
        "size": size,
        "query": {
            "function_score": {
                "query": bool_query,
                "functions": [
                    {
                        # Libros con mejor nota suben en el ranking
                        "field_value_factor": {
                            "field":    "Rating",
                            "factor":   1.2,
                            # CAMBIO: ln2p es positivo incluso si Rating es 0.
                            "modifier": "ln2p",
                            "missing":  0,
                        },
                        "weight": 0.15,  # CAMBIO: mantener el rating secundario al texto.
                    },
                    {
                        # Libros con más reseñas suben (popularidad)
                        "field_value_factor": {
                            "field":    "CountsOfReview",
                            "factor":   0.5,
                            # CAMBIO: 0 reseñas no puede anular la relevancia textual.
                            "modifier": "ln2p",
                            "missing":  0,
                        },
                        "weight": 0.05,
                    },
                ],
                # CAMBIO: sumar factores evita que un valor cero borre el score de texto.
                "score_mode":   "sum",
                "boost_mode":   "sum",
            }
        },
        # Highlight: resaltar coincidencias en los campos textuales
        "highlight": {
            "fields": {
                "Description":     {"fragment_size": 150, "number_of_fragments": 2},
                "Name":            {},
                "Authors":         {},
                "searchable_text": {"fragment_size": 150, "number_of_fragments": 1},
            },
            "pre_tags":  ["<<"],
            "post_tags": [">>"],
        },
        # Campos a devolver
        "_source": [
            "Id",  # CAMBIO: exponer el identificador para rastrear los resultados.
            "Name", "Authors", "Rating", "PublishYear",
            "Language", "Publisher", "CountsOfReview",
            "pagesNumber", "Description",
            "RatingDist1", "RatingDist2", "RatingDist3",
            "RatingDist4", "RatingDist5", "RatingDistTotal",
        ],
    }
    return query


def format_rating_bar(src):
    """Genera una barra visual con la distribución de calificaciones."""
    dists = []
    for i in range(1, 6):
        val = src.get(f"RatingDist{i}", 0) or 0
        dists.append(val)
    total = sum(dists) or 1
    bar = ""
    for i, v in enumerate(dists, 1):
        pct = v / total * 100
        blocks = int(pct / 5)
        bar += f"  {'★' * i}{'☆' * (5-i)} {v:>7,d} ({pct:4.1f}%) {'█' * blocks}"
        bar += "\n"
    return bar


def print_results(response, query_text):
    """Muestra los resultados de forma legible."""
    hits = response["hits"]
    total = hits["total"]["value"] if isinstance(hits["total"], dict) else hits["total"]
    print(f"\n🔍 Búsqueda: \"{query_text}\"")
    print(f"   Resultados encontrados: {total}")
    print(f"{'─' * 70}")

    if not hits["hits"]:
        print("   No se encontraron resultados.")
        return

    for i, hit in enumerate(hits["hits"], 1):
        src   = hit["_source"]
        score = hit["_score"]
        hl    = hit.get("highlight", {})

        name    = src.get("Name", "Sin título")
        authors = src.get("Authors", "Desconocido")
        rating  = src.get("Rating")
        year    = src.get("PublishYear")
        lang    = src.get("Language", "?")
        pub     = src.get("Publisher", "")
        reviews = src.get("CountsOfReview", 0)
        pages   = src.get("pagesNumber")

        # Encabezado del resultado
        # CAMBIO: 0.0 es un rating válido; no equivale a un valor ausente.
        rating_str = f"{'★' * int(rating)}{'☆' * (5 - int(rating))} {rating:.2f}" if rating is not None else "Sin rating"
        year_str   = str(year) if year else "?"
        pages_str  = f"{pages} págs." if pages else ""

        print(f"\n  {i}. {name}")
        print(f"     Autor(es):  {authors}")
        print(f"     {rating_str}  |  {year_str}  |  {lang}  |  {pages_str}")
        if pub:
            print(f"     Editorial:  {pub}")
        if reviews:
            print(f"     Reseñas:    {reviews:,d}")

        # Distribución de calificaciones
        total_ratings = src.get("RatingDistTotal", 0)
        if total_ratings and total_ratings > 0:
            print(f"     Distribución ({total_ratings:,d} votos):")
            bar = format_rating_bar(src)
            for line in bar.strip().split("\n"):
                print(f"       {line}")

        # Destacados (highlights)
        if hl:
            print(f"     Coincidencias:")
            for field, fragments in hl.items():
                label = field.replace("searchable_text", "texto completo")
                for frag in fragments:
                    print(f"       [{label}] ...{frag}...")

        print(f"     (score: {score:.4f})")
        print(f"{'─' * 70}")


def interactive_mode(es, index, size):
    """Modo interactivo: bucle de búsqueda."""
    print("=" * 70)
    print("  Motor de Búsqueda Semántica de Libros — Goodreads")
    print("=" * 70)
    print("  Escribe tu búsqueda y presiona Enter.")
    print("  Filtros opcionales (agrégalos a la consulta):")
    print("    lang:eng          → filtrar por idioma")
    print("    year:2000-2020    → rango de años")
    print("    rating:4.0        → rating mínimo")
    print("  Escribe 'salir' para terminar.\n")

    while True:
        try:
            raw = input("🔎 > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n¡Hasta luego!")
            break

        if not raw or raw.lower() in ("salir", "exit", "quit"):
            print("¡Hasta luego!")
            break

        # Parsear filtros inline
        lang = min_year = max_year = min_rating = None
        tokens = raw.split()
        query_tokens = []
        # CAMBIO: filtros mal escritos no deben cerrar la interfaz interactiva.
        try:
            for t in tokens:
                if t.startswith("lang:"):
                    lang = t.split(":", 1)[1]
                elif t.startswith("year:"):
                    yr = t.split(":", 1)[1]
                    if "-" in yr:
                        parts = yr.split("-")
                        min_year = int(parts[0]) if parts[0] else None
                        max_year = int(parts[1]) if parts[1] else None
                    else:
                        min_year = max_year = int(yr)
                elif t.startswith("rating:"):
                    min_rating = float(t.split(":", 1)[1])
                else:
                    query_tokens.append(t)
        except ValueError:
            print("  Filtro inválido: revisa year:AAAA-AAAA o rating:N.N")
            continue

        query_text = " ".join(query_tokens)
        if not query_text:
            print("  Escribe algo para buscar.")
            continue

        body = build_query(query_text, lang, min_year, max_year, min_rating, size)
        response = es.search(index=index, body=body)
        print_results(response, query_text)


# =============================================================================
# Main
# =============================================================================
if __name__ == "__main__":
    es = Elasticsearch(f"http://{args.es_host}")
    if not es.ping():
        print(f"ERROR: No se pudo conectar a Elasticsearch en {args.es_host}")
        sys.exit(1)

    if args.query:
        # Modo directo: una sola búsqueda
        body = build_query(
            args.query, args.lang, args.min_year, args.max_year,
            args.min_rating, args.size,
        )
        if args.debug:
            print("Query enviada:")
            print(json.dumps(body, indent=2, ensure_ascii=False))
        response = es.search(index=args.index, body=body)
        print_results(response, args.query)
    else:
        # Modo interactivo
        interactive_mode(es, args.index, args.size)
