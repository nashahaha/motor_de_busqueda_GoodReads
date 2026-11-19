GNU nano 8.4                                           muestra.py
# Muestra de desarrollo: limita raw_union a N filas
# Ejecutar: spark-submit muestra.py [N]   (por defecto 5000)

import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    spark = SparkSession.builder.appName("grupo8_muestra").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet("hdfs:///uhadoop2026/grupo8/raw_union")
    print("COLUMNAS:", df.columns)
    print("TOTAL raw_union:", df.count())

    muestra = df.limit(n)
    muestra.write.mode("overwrite").parquet("hdfs:///uhadoop2026/grupo8/sample_books")
    print("MUESTRA escrita en hdfs:///uhadoop2026/grupo8/sample_books (", n, "filas )")
    muestra.show(5, truncate=40)