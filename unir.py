# Script de unión de los archivos que sirven
# Se ejecuta con spark-submit unir.py 2>/dev/null

import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("grupo8_unir").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

CANON = ["Id", "Name", "Authors", "ISBN", "Rating", "PublishYear", "PublishMonth",
         "PublishDay", "Publisher", "RatingDist1", "RatingDist2", "RatingDist3",
         "RatingDist4", "RatingDist5", "RatingDistTotal", "CountsOfReview",
         "Language", "pagesNumber", "Description"]
LOWER = {c.lower(): c for c in CANON}

out = subprocess.check_output(["hdfs", "dfs", "-ls", "/uhadoop2026/grupo8/"]).decode()
files = sorted(l.split()[-1] for l in out.splitlines()
               if "/book" in l and l.endswith(".csv"))

union = None
for f in files:
    d = spark.read.csv("hdfs://" + f, header=True, multiLine=True, escape='"')
    if "description" not in [c.lower() for c in d.columns]:
        print("SALTADO (sin Description):", f.split("/")[-1])
        continue
    # renombrar a nombres canónicos y descartar columnas extra
    d = d.select([col("`%s`" % c).alias(LOWER[c.lower()])
                  for c in d.columns if c.lower() in LOWER])
    d = d.select(CANON)  # mismo orden en todos
    print("OK:", f.split("/")[-1], len(d.columns))
    union = d if union is None else union.unionByName(d)

union.write.mode("overwrite").parquet("hdfs:///uhadoop2026/grupo8/raw_union")

u = spark.read.parquet("hdfs:///uhadoop2026/grupo8/raw_union")
print("TOTAL FILAS:", u.count())
u.select("Id", "Name", "Authors", "Language", "PublishYear", "Rating").show(5, truncate=40)
u.groupBy("Language").count().orderBy("count", ascending=False).show(10)