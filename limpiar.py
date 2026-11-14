# Fase 1 — Limpieza y normalización con Spark
# Uso: spark-submit --master spark://cluster-01:7077 limpiar.py <input_parquet> <output_parquet> [output_json_dir]
# Primero sobre la muestra, luego sobre raw_union. Para datos completos usar siempre --master spark://cluster-01:7077.

import html
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

FINAL = ["Id", "Name", "Authors", "Description", "Language", "Publisher",
         "PublishYear", "Rating",
         "RatingDist1", "RatingDist2", "RatingDist3", "RatingDist4", "RatingDist5",
         "RatingDistTotal", "CountsOfReview"]

VACIO = ["", "####", "null", "none", "nan", "n/a"]


def _txt(s):
    if s is None:
        return None
    s = str(s).strip()
    return None if s.lower() in VACIO else s


def parse_anio(s):
    s = _txt(s)
    if s is None:
        return None
    try:
        y = int(float(s))
    except ValueError:
        return None
    if y < 1000 or y > datetime.now().year:
        return None
    return y


def parse_rating(s):
    s = _txt(s)
    if s is None:
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    if v < 0.0 or v > 5.0:
        return None
    return v


def parse_int(s):
    s = _txt(s)
    if s is None:
        return None
    try:
        v = int(float(s))
    except ValueError:
        return None
    return v if v >= 0 else None


def parse_rating_dist(s):
    # formato observado: "1:0", "5:10" en RatingDist1..5 y "total:62" en RatingDistTotal
    s = _txt(s)
    if s is None:
        return None
    if ":" in s:
        s = s.split(":", 1)[1].strip()
    try:
        v = int(float(s))
    except ValueError:
        return None
    return v if v >= 0 else None


def main():
    if len(sys.argv) < 3:
        print("Uso: spark-submit limpiar.py <input_parquet> <output_parquet> [output_json_dir]")
        sys.exit(-1)
    inp, outp = sys.argv[1], sys.argv[2]

    spark = SparkSession.builder.appName("grupo8_limpiar").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(inp)
    total = df.count()
    print("FILAS INICIALES:", total)

    df = df.select("Id", "Name", "Authors", "Description", "Language", "Publisher",
                   "PublishYear", "Rating",
                   "RatingDist1", "RatingDist2", "RatingDist3", "RatingDist4", "RatingDist5",
                   "RatingDistTotal", "CountsOfReview")

    anio = F.udf(parse_anio, IntegerType())
    rating = F.udf(parse_rating, DoubleType())
    entero = F.udf(parse_int, IntegerType())
    dist = F.udf(parse_rating_dist, IntegerType())
    unescape = F.udf(lambda s: html.unescape(str(s)) if s is not None else None, StringType())

    desc = F.col("Description")
    desc_t = F.trim(desc)
    desc = F.when(F.lower(desc_t).isin(VACIO), F.lit(None)).otherwise(desc_t)
    desc = unescape(desc)
    desc = F.regexp_replace(desc, "<[^>]*>", " ")
    desc = F.regexp_replace(desc, r"\s+", " ")
    desc = F.trim(desc)
    desc = F.when(desc == "", F.lit(None)).otherwise(desc)

    aut = F.col("Authors")
    aut_t = F.trim(F.regexp_replace(aut, r"\s+", " "))
    aut = F.when(F.lower(aut_t).isin(VACIO), F.lit(None)).otherwise(aut_t)

    lang = F.col("Language")
    lang_t = F.trim(F.lower(lang))
    lang = F.when(lang_t.isin(VACIO) | (lang_t == "") | lang_t.isNull(), F.lit("unknown")).otherwise(lang_t)

    df = df.withColumn("PublishYear", anio(F.col("PublishYear"))) \
           .withColumn("Rating", rating(F.col("Rating"))) \
           .withColumn("CountsOfReview", entero(F.col("CountsOfReview")).cast("long")) \
           .withColumn("RatingDist1", dist(F.col("RatingDist1")).cast("long")) \
           .withColumn("RatingDist2", dist(F.col("RatingDist2")).cast("long")) \
           .withColumn("RatingDist3", dist(F.col("RatingDist3")).cast("long")) \
           .withColumn("RatingDist4", dist(F.col("RatingDist4")).cast("long")) \
           .withColumn("RatingDist5", dist(F.col("RatingDist5")).cast("long")) \
           .withColumn("RatingDistTotal", dist(F.col("RatingDistTotal")).cast("long")) \
           .withColumn("Description", desc) \
           .withColumn("Language", lang) \
           .withColumn("Authors", aut)

    sin_nombre = df.filter(F.col("Name").isNull() | (F.trim(F.col("Name")) == "")).count()
    print("DESCARTADAS (sin Name):", sin_nombre)
    df = df.filter(F.col("Name").isNotNull() & (F.trim(F.col("Name")) != ""))

    con_dups = df.groupBy("Id").count().filter(F.col("count") > 1).count()
    print("ID DUPLICADOS (grupos):", con_dups)
    df = df.dropDuplicates(["Id"])

    stats = df.agg(F.count("*").alias("final"),
                   F.sum(F.col("Description").isNull().cast("int")).alias("desc_vacias")).collect()[0]
    final, desc_vacias = stats["final"], stats["desc_vacias"]
    print("FILAS FINALES:", final)
    print("DESCRIPCIONES VACÍAS/NULAS:", desc_vacias, "(%.2f%%)" % (100.0 * desc_vacias / final if final else 0))
    print("TOP 10 IDIOMAS:")
    df.groupBy("Language").count().orderBy(F.desc("count")).show(10)

    df.write.mode("overwrite").parquet(outp)
    if len(sys.argv) > 3:
        df.write.mode("overwrite").json(sys.argv[3])
        print("JSONL escrito en", sys.argv[3])


if __name__ == "__main__":
    main()
