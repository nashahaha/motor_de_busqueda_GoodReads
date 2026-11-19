# =============================================================================
# 02_limpiar.py — Limpieza y normalización con Apache Spark
# Lee raw_union, limpia campos, deduplica, crea searchable_text.
# Ejecutar:  spark-submit 02_limpiar.py 2>/dev/null
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder.appName("grupo8_limpiar").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

INPUT  = "hdfs:///uhadoop2026/grupo8/raw_union"
OUTPUT = "hdfs:///uhadoop2026/grupo8/clean_books"

df = spark.read.parquet(INPUT)
print(f"Filas leídas de raw_union: {df.count()}")

# =============================================================================
# 1. Parsear campos RatingDist  ("5:891037" → 891037)
# =============================================================================
for rc in ["RatingDist1", "RatingDist2", "RatingDist3",
           "RatingDist4", "RatingDist5", "RatingDistTotal"]:
    df = df.withColumn(
        rc,
        F.split(F.col(rc), ":").getItem(1).cast(IntegerType())
    )

# =============================================================================
# 2. Castear campos numéricos
# =============================================================================
df = (df
      .withColumn("Rating",         F.col("Rating").cast(FloatType()))
      .withColumn("PublishYear",    F.col("PublishYear").cast(IntegerType()))
      .withColumn("PublishMonth",   F.col("PublishMonth").cast(IntegerType()))
      .withColumn("PublishDay",     F.col("PublishDay").cast(IntegerType()))
      .withColumn("CountsOfReview", F.col("CountsOfReview").cast(IntegerType()))
      .withColumn("pagesNumber",    F.col("pagesNumber").cast(IntegerType()))
      )

# =============================================================================
# 3. Limpiar textos (trim de espacios y saltos de línea extra)
# =============================================================================
for tc in ["Name", "Authors", "Publisher", "Description"]:
    df = df.withColumn(tc, F.trim(F.regexp_replace(F.col(tc), r"\s+", " ")))

# =============================================================================
# 4. Filtros básicos de calidad
# =============================================================================
antes = df.count()

# Descartar filas sin título
df = df.filter(F.col("Name").isNotNull() & (F.length(F.col("Name")) > 0))

# Año razonable (si existe): entre 1000 y 2026
df = df.filter(
    F.col("PublishYear").isNull() |
    ((F.col("PublishYear") >= 1000) & (F.col("PublishYear") <= 2026))
)

# Rating entre 0 y 5 (si existe)
df = df.filter(
    F.col("Rating").isNull() |
    ((F.col("Rating") >= 0.0) & (F.col("Rating") <= 5.0))
)

despues_filtro = df.count()
print(f"Filas eliminadas por filtros de calidad: {antes - despues_filtro}")

# =============================================================================
# 5. Deduplicar por Id (mantiene primera ocurrencia)
# =============================================================================
antes_dedup = df.count()
df = df.dropDuplicates(["Id"])
despues_dedup = df.count()
print(f"Duplicados eliminados: {antes_dedup - despues_dedup}")

# =============================================================================
# 6. Crear campo searchable_text (concatenación de campos textuales)
# =============================================================================
df = df.withColumn(
    "searchable_text",
    F.trim(F.concat_ws(
        " ",
        F.coalesce(F.col("Name"),        F.lit("")),
        F.coalesce(F.col("Authors"),     F.lit("")),
        F.coalesce(F.col("Publisher"),   F.lit("")),
        F.coalesce(F.col("Description"), F.lit("")),
    ))
)

# =============================================================================
# 7. Escribir resultado limpio
# =============================================================================
df.write.mode("overwrite").parquet(OUTPUT)

# --- Estadísticas finales -----------------------------------------------------
clean = spark.read.parquet(OUTPUT)
total         = clean.count()
con_desc      = clean.filter(F.col("Description").isNotNull()).count()
con_publisher = clean.filter(F.col("Publisher").isNotNull()).count()
con_lang      = clean.filter(F.col("Language").isNotNull()).count()

print(f"\n{'='*60}")
print(f"TOTAL FILAS LIMPIAS:     {total}")
print(f"  Con Description:       {con_desc}  ({100*con_desc/total:.1f}%)")
print(f"  Con Publisher:         {con_publisher}  ({100*con_publisher/total:.1f}%)")
print(f"  Con Language:          {con_lang}  ({100*con_lang/total:.1f}%)")
print(f"Escrito en:              {OUTPUT}")
print(f"{'='*60}")

clean.select("Id", "Name", "Authors", "Rating", "searchable_text").show(5, truncate=60)

# Distribución de idiomas
print("Top 10 idiomas:")
clean.groupBy("Language").count().orderBy("count", ascending=False).show(10)

spark.stop()
