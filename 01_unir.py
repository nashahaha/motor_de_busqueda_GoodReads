# =============================================================================
# 01_unir.py — Unión de TODOS los archivos book*.csv
# Incluye archivos sin columna Description (la agrega como null).
# Ejecutar:  spark-submit --master spark://cluster-01:7077 01_unir.py
# =============================================================================

import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("grupo8_unir").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# --- Esquema canónico ---------------------------------------------------------
CANON = [
    "Id", "Name", "Authors", "ISBN", "Rating",
    "PublishYear", "PublishMonth", "PublishDay", "Publisher",
    "RatingDist1", "RatingDist2", "RatingDist3",
    "RatingDist4", "RatingDist5", "RatingDistTotal",
    "CountsOfReview", "Language", "pagesNumber", "Description",
]
LOWER = {c.lower(): c for c in CANON}

# --- Listar archivos book*.csv en HDFS ----------------------------------------
out = subprocess.check_output(
    ["hdfs", "dfs", "-ls", "/uhadoop2026/grupo8/"]
).decode()
files = sorted(
    l.split()[-1]
    for l in out.splitlines()
    # CAMBIO: filtrar por nombre de archivo, no por coincidencias en la ruta HDFS.
    if l.split()[-1].rsplit("/", 1)[-1].startswith("book") and l.endswith(".csv")
)
print(f"Archivos encontrados: {len(files)}")
# CAMBIO: evitar intentar escribir un DataFrame inexistente cuando faltan CSV.
if not files:
    raise SystemExit("No se encontraron archivos book*.csv en /uhadoop2026/grupo8/")

# --- Leer y unir --------------------------------------------------------------
union = None
for f in files:
    d = spark.read.csv("hdfs://" + f, header=True, multiLine=True, escape='"')
    cols_lower = [c.lower() for c in d.columns]

    # Si no tiene Description, la agregamos como columna nula
    if "description" not in cols_lower:
        d = d.withColumn("Description", lit(None).cast("string"))
        print(f"  + {f.split('/')[-1]:30s}  (sin Description → agregada como null)")
    else:
        print(f"  + {f.split('/')[-1]:30s}  (con Description)")

    # Renombrar a nombres canónicos, descartar columnas extra
    d = d.select(
        [col("`%s`" % c).alias(LOWER[c.lower()])
         for c in d.columns if c.lower() in LOWER]
    )
    # Asegurar el mismo orden de columnas en todos los DataFrames
    for canon_col in CANON:
        if canon_col not in d.columns:
            d = d.withColumn(canon_col, lit(None).cast("string"))
    d = d.select(CANON)

    union = d if union is None else union.unionByName(d)

# --- Escribir parquet ----------------------------------------------------------
OUTPUT = "hdfs:///uhadoop2026/grupo8/raw_union"
union.write.mode("overwrite").parquet(OUTPUT)

u = spark.read.parquet(OUTPUT)
total = u.count()
con_desc = u.filter(col("Description").isNotNull()).count()
print(f"\n{'='*60}")
print(f"TOTAL FILAS:          {total}")
# CAMBIO: evitar dividir por cero si la unión no contiene registros.
print(f"Con Description:      {con_desc}  ({100*con_desc/total if total else 0:.1f}%)")
print(f"Sin Description:      {total - con_desc}")
print(f"Escrito en:           {OUTPUT}")
print(f"{'='*60}")
u.select("Id", "Name", "Authors", "Language", "PublishYear", "Rating").show(5, truncate=40)

spark.stop()
