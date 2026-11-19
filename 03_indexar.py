# =============================================================================
# 03_indexar.py — Indexación de libros en Elasticsearch
# Lee el parquet limpio con Spark e indexa en ES con elasticsearch-py.
#
# Ejecutar:
#   spark-submit 03_indexar.py                           # ES en localhost:9200
#   spark-submit 03_indexar.py --es-host mihost:9200     # ES en otro host
#   spark-submit 03_indexar.py --recreate                # Borra y recrea el índice
#
# Requisito: pip install elasticsearch
# =============================================================================

import argparse
import sys
import json
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# --- Argumentos ---------------------------------------------------------------
parser = argparse.ArgumentParser(description="Indexar libros en Elasticsearch")
parser.add_argument("--es-host",  default="localhost:9200", help="Host:puerto de ES")
parser.add_argument("--index",    default="goodreads_books", help="Nombre del índice")
parser.add_argument("--recreate", action="store_true", help="Borrar índice si existe")
parser.add_argument("--batch",    type=int, default=500, help="Tamaño del lote de bulk")
args = parser.parse_args()

ES_HOST    = args.es_host
INDEX_NAME = args.index
BATCH_SIZE = args.batch

# =============================================================================
# 1. Mapping del índice
# =============================================================================
INDEX_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index.mapping.total_fields.limit": 50,
    },
    "mappings": {
        "properties": {
            # --- Campos textuales (buscables) ---
            "Name": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}
            },
            "Authors": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
            },
            "Publisher": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
            },
            "Description":     {"type": "text", "analyzer": "standard"},
            "searchable_text": {"type": "text", "analyzer": "standard"},

            # --- Campos estructurados (filtrables / ordenables) ---
            "Id":              {"type": "keyword"},
            "ISBN":            {"type": "keyword"},
            "Language":        {"type": "keyword"},
            "Rating":          {"type": "float"},
            "PublishYear":     {"type": "integer"},
            "PublishMonth":    {"type": "integer"},
            "PublishDay":      {"type": "integer"},
            "pagesNumber":     {"type": "integer"},
            "CountsOfReview":  {"type": "integer"},
            "RatingDist1":     {"type": "integer"},
            "RatingDist2":     {"type": "integer"},
            "RatingDist3":     {"type": "integer"},
            "RatingDist4":     {"type": "integer"},
            "RatingDist5":     {"type": "integer"},
            "RatingDistTotal": {"type": "integer"},
        }
    }
}

# =============================================================================
# 2. Conectar a Elasticsearch y crear el índice
# =============================================================================
es = Elasticsearch(f"http://{ES_HOST}")

if not es.ping():
    print(f"ERROR: No se pudo conectar a Elasticsearch en {ES_HOST}")
    sys.exit(1)
print(f"Conectado a Elasticsearch en {ES_HOST}")

if es.indices.exists(index=INDEX_NAME):
    if args.recreate:
        es.indices.delete(index=INDEX_NAME)
        print(f"Índice '{INDEX_NAME}' eliminado.")
    else:
        print(f"El índice '{INDEX_NAME}' ya existe. Usa --recreate para recrearlo.")
        sys.exit(1)

es.indices.create(index=INDEX_NAME, body=INDEX_SETTINGS)
print(f"Índice '{INDEX_NAME}' creado.")

# =============================================================================
# 3. Leer datos limpios con Spark
# =============================================================================
spark = SparkSession.builder.appName("grupo8_indexar").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

INPUT = "hdfs:///uhadoop2026/grupo8/clean_books"
df = spark.read.parquet(INPUT)
total = df.count()
print(f"Libros a indexar: {total}")

# =============================================================================
# 4. Indexar en lotes con bulk API
# =============================================================================
indexed = 0
errors  = 0

def generate_actions(rows):
    """Genera acciones de bulk a partir de una lista de Row de Spark."""
    for row in rows:
        doc = row.asDict()
        doc_id = doc.pop("Id", None)
        # Remover campos con valor None para no enviar nulls innecesarios
        doc = {k: v for k, v in doc.items() if v is not None}
        yield {
            "_index":  INDEX_NAME,
            "_id":     doc_id,
            "_source": doc,
        }

# Recorrer particiones una a una para no saturar memoria del driver
iterator = df.toLocalIterator()
batch = []

for row in iterator:
    batch.append(row)
    if len(batch) >= BATCH_SIZE:
        success, err_list = bulk(es, generate_actions(batch),
                                 raise_on_error=False, stats_only=True)
        indexed += success
        errors  += err_list
        print(f"  Indexados: {indexed}/{total}  (errores: {errors})", end="\r")
        batch = []

# Último lote
if batch:
    success, err_list = bulk(es, generate_actions(batch),
                             raise_on_error=False, stats_only=True)
    indexed += success
    errors  += err_list

# Refrescar el índice para que los documentos sean buscables
es.indices.refresh(index=INDEX_NAME)

print(f"\n{'='*60}")
print(f"INDEXACIÓN COMPLETA")
print(f"  Documentos indexados: {indexed}")
print(f"  Errores:              {errors}")
print(f"  Índice:               {INDEX_NAME}")
print(f"{'='*60}")

spark.stop()
