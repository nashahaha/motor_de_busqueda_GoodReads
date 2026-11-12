# Para ver los datos
# Se ejecuta con spark-submit explorar.py 2>/dev/null

import subprocess
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("grupo8_explorar").getOrCreate()

# Listar archivos book*.csv en HDFS
out = subprocess.check_output(["hdfs", "dfs", "-ls", "/uhadoop2026/grupo8/"]).decode()
files = [l.split()[-1] for l in out.splitlines() if "/book" in l and l.endswith(".csv")]

# Ver las columnas de cada archivo
for f in sorted(files):
    d = spark.read.csv("hdfs://" + f, header=True, multiLine=True, escape='"')
    print(f.split("/")[-1], len(d.columns), d.columns)