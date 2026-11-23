# =============================================================================
# 03_indexar.py — Indexación de libros en Elasticsearch
# Lee el parquet limpio con Spark e indexa en ES con elasticsearch-py.
#
# Ejecutar:
#   spark-submit 03_indexar.py                           # ES en cm:9200
#   spark-submit 03_indexar.py --es-host mihost:9200     # ES en otro host
#   spark-submit 03_indexar.py --recreate                # Borra y recrea el índice
#
# Requisito: pip install "elasticsearch>=6,<7"  # cliente compatible con ES 6.8
# =============================================================================

import argparse
import sys
import json
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# --- Argumentos ---------------------------------------------------------------
parser = argparse.ArgumentParser(description="Indexar libros en Elasticsearch")
# CAMBIO: el servicio del laboratorio está en cm:9200, no en localhost.
parser.add_argument("--es-host",  default="cm:9200", help="Host:puerto de ES")
# CAMBIO: usar un índice propio del grupo para no pisar índices de otros usuarios.
parser.add_argument("--index",    default="grupo8_goodreads_books", help="Nombre del índice")
parser.add_argument("--recreate", action="store_true", help="Borrar índice si existe")
parser.add_argument("--batch",    type=int, default=500, help="Tamaño del lote de bulk")
# CAMBIO: permitir ensayar con un subconjunto antes de indexar todo.
parser.add_argument("--limit",    type=int, default=None, help="Máximo de libros a indexar (pruebas)")
args = parser.parse_args()
if args.batch < 1 or (args.limit is not None and args.limit < 1):
    parser.error("--batch y --limit deben ser mayores que cero")
# CAMBIO: una muestra nunca debe reemplazar por error el índice de producción.
if args.limit is not None and args.index == "grupo8_goodreads_books":
    parser.error("para --limit indica --index grupo8_goodreads_test (índice de prueba)")

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
        # CAMBIO: ES 6.8 espera un tipo de mapping (_doc) por defecto.
        "_doc": {
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
                "RatingDist1":     {"type": "long"},
                "RatingDist2":     {"type": "long"},
                "RatingDist3":     {"type": "long"},
                "RatingDist4":     {"type": "long"},
                "RatingDist5":     {"type": "long"},
                "RatingDistTotal": {"type": "long"},
            }
        }
    }
}

# =============================================================================
# 2. Conectar a Elasticsearch
# =============================================================================
es = Elasticsearch(f"http://{ES_HOST}")

if not es.ping():
    print(f"ERROR: No se pudo conectar a Elasticsearch en {ES_HOST}")
    sys.exit(1)
print(f"Conectado a Elasticsearch en {ES_HOST}")

# CAMBIO: verificar versión y datos ANTES de cualquier borrado (--recreate).
# Esto evita perder el índice anterior cuando no hay Parquet para procesar.
major = int(es.info()["version"]["number"].split(".")[0])
if major != 6:
    sys.exit(f"ERROR: este mapping requiere Elasticsearch 6.x; servidor detectado: {major}.x")

# =============================================================================
# 3. Leer datos limpios con Spark
# =============================================================================
spark = SparkSession.builder.appName("grupo8_indexar").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

INPUT = "hdfs:///uhadoop2026/grupo8/clean_books"
df = spark.read.parquet(INPUT)
if args.limit is not None:
    df = df.limit(args.limit)
total = df.count()
if total == 0:
    spark.stop()  # CAMBIO: liberar recursos de Spark también al abortar.
    sys.exit("ERROR: no hay libros para indexar; no se modificó el índice")
print(f"Libros a indexar: {total}")

# CAMBIO: ahora que se validaron los datos, puede crearse o recrearse el índice.
if es.indices.exists(index=INDEX_NAME):
    if args.recreate:
        es.indices.delete(index=INDEX_NAME)
        print(f"Índice '{INDEX_NAME}' eliminado.")
    else:
        print(f"El índice '{INDEX_NAME}' ya existe. Usa --recreate para recrearlo.")
        spark.stop()  # CAMBIO: liberar Spark al abortar sin recrear el índice.
        sys.exit(1)

es.indices.create(index=INDEX_NAME, body=INDEX_SETTINGS)
print(f"Índice '{INDEX_NAME}' creado.")

# =============================================================================
# 4. Indexar en lotes con bulk API
# =============================================================================
indexed = 0
errors  = 0

def generate_actions(rows):
    """Genera acciones de bulk a partir de una lista de Row de Spark."""
    for row in rows:
        doc = row.asDict()
        # CAMBIO: no quitar Id del _source; permite identificar libros en los resultados.
        doc_id = str(doc.get("Id", "")).strip() if doc.get("Id") is not None else ""
        if not doc_id:
            raise ValueError("Libro sin Id: revisar 02_limpiar.py antes de indexar")
        # Remover campos con valor None para no enviar nulls innecesarios
        doc = {k: v for k, v in doc.items() if v is not None}
        yield {
            "_index":  INDEX_NAME,
            "_type":   "_doc",  # CAMBIO: tipo requerido por el servidor ES 6.x.
            "_id":     doc_id,
            "_source": doc,
        }

# Recorrer particiones una a una para no saturar memoria del driver
iterator = df.toLocalIterator()
batch = []

for row in iterator:
    batch.append(row)
    if len(batch) >= BATCH_SIZE:
        # CAMBIO: guardar detalles de los errores para poder diagnosticar fallos de bulk.
        success, err_list = bulk(es, generate_actions(batch),
                                 raise_on_error=False, request_timeout=60)
        indexed += success
        errors  += len(err_list)
        for error in err_list[:3]:
            print(f"\nERROR BULK: {str(error)[:500]}")
        print(f"  Indexados: {indexed}/{total}  (errores: {errors})", end="\r")
        batch = []

# Último lote
if batch:
    success, err_list = bulk(es, generate_actions(batch),
                             raise_on_error=False, request_timeout=60)
    indexed += success
    errors  += len(err_list)
    for error in err_list[:3]:
        print(f"\nERROR BULK: {str(error)[:500]}")

# Refrescar el índice para que los documentos sean buscables
es.indices.refresh(index=INDEX_NAME)

print(f"\n{'='*60}")
print(f"INDEXACIÓN COMPLETA")
print(f"  Documentos indexados: {indexed}")
print(f"  Errores:              {errors}")
print(f"  Índice:               {INDEX_NAME}")
print(f"{'='*60}")

spark.stop()
# CAMBIO: una indexación con errores no debe terminar con estado exitoso.
if errors:
    sys.exit(1)
