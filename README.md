## Grupo

Grupo 8
- Benjamín Brito
- Ignacia Galaz Alvarado
- Pablo Osorio
  
# Motor de Búsqueda Semántica de Libros — Goodreads

Motor de búsqueda sobre Elasticsearch que permite recuperar libros por
coincidencia en título, autor, editorial y **palabras clave presentes en la
descripción** (ej: "misterio", "superación personal", "viaje espacial").

## Dataset

[Goodreads Book Datasets With User Rating 10M](https://www.kaggle.com/datasets/bahramjannesarr/goodreads-book-datasets-10m) (Kaggle).

Se utilizan **todos los archivos `book*.csv`** (23 archivos). Los 17 archivos
desde `book600k-700k.csv` tienen la columna `Description`; los 6 anteriores se
incluyen con `Description = null` y siguen siendo buscables por título, autor y
editorial.

## Tecnologías

| Componente       | Tecnología              |
|------------------|-------------------------|
| Limpieza / ETL   | Apache Spark (PySpark)  |
| Búsqueda         | Elasticsearch           |
| Interfaz         | Python CLI              |

## Arquitectura

```
book*.csv (HDFS)
     │
     ▼
┌──────────────┐    ┌───────────────┐    ┌───────────────┐
│  01_unir.py  │───▶│ 02_limpiar.py │───▶│ 03_indexar.py │
│  (Spark)     │    │  (Spark)      │    │ (Spark + ES)  │
└──────────────┘    └───────────────┘    └───────┬───────┘
  raw_union.parquet   clean_books.parquet         │
                                                  ▼
                                         ┌───────────────┐
                                         │ Elasticsearch  │
                                         │ (goodreads_    │
                                         │   books)       │
                                         └───────┬───────┘
                                                  │
                                                  ▼
                                         ┌───────────────┐
                                         │ 04_buscar.py  │
                                         │ (Python CLI)  │
                                         └───────────────┘
```

## Ejecución paso a paso

### Prerequisitos

```bash
pip install elasticsearch
```

### Paso 1 — Unir archivos CSV

Lee todos los `book*.csv` de HDFS, normaliza columnas y los une en un
Parquet unificado. Los archivos sin columna `Description` se incluyen con
ese campo en null.

```bash
spark-submit 01_unir.py 2>/dev/null
```

**Entrada:** `hdfs:///uhadoop2026/grupo8/book*.csv`
**Salida:**  `hdfs:///uhadoop2026/grupo8/raw_union` (Parquet)

### Paso 2 — Limpiar y normalizar

Parsea campos numéricos (`RatingDist*`, `Rating`, `PublishYear`, etc.),
elimina duplicados, filtra datos inválidos y crea el campo
`searchable_text` que concatena título + autores + editorial + descripción.

```bash
spark-submit 02_limpiar.py 2>/dev/null
```

**Entrada:** `hdfs:///uhadoop2026/grupo8/raw_union`
**Salida:**  `hdfs:///uhadoop2026/grupo8/clean_books` (Parquet)

### Paso 3 — Indexar en Elasticsearch

Crea el índice con el mapping adecuado e indexa todos los libros.

```bash
# Elasticsearch en localhost:9200 (por defecto)
spark-submit 03_indexar.py --recreate 2>/dev/null

# Elasticsearch en otro host
spark-submit 03_indexar.py --es-host mihost:9200 --recreate 2>/dev/null
```

**Entrada:** `hdfs:///uhadoop2026/grupo8/clean_books`
**Salida:**  Índice `goodreads_books` en Elasticsearch

### Paso 4 — Buscar

```bash
# Búsqueda directa
python 04_buscar.py "misterio"
python 04_buscar.py "viaje espacial" --lang eng
python 04_buscar.py "García Márquez" --min-year 1960 --max-year 2000
python 04_buscar.py "superación personal" --min-rating 4.0 --size 20

# Modo interactivo
python 04_buscar.py
```

## Estrategia de búsqueda

### Multi-campo con boosting

La búsqueda no depende de un solo campo. Cada consulta se evalúa en
múltiples campos con pesos diferenciados:

| Campo              | Boost | Razón                                           |
|--------------------|-------|-------------------------------------------------|
| `Description`      | ×3    | Contiene palabras clave semánticas               |
| `Name`             | ×2    | El título es muy relevante                       |
| `Authors`          | ×1.5  | Permite buscar por autor                         |
| `Publisher`        | ×1    | Complementario                                   |
| `searchable_text`  | ×1    | Red de seguridad (siempre tiene contenido)       |

### Function score (ranking inteligente)

Además de la relevancia textual, el score se ajusta por:

- **Rating** (nota promedio): libros mejor valorados suben en el ranking
- **CountsOfReview** (popularidad): libros con más reseñas suben

### Filtros opcionales

- `--lang`: filtrar por idioma (`eng`, `spa`, `fre`, etc.)
- `--min-year` / `--max-year`: rango de años de publicación
- `--min-rating`: nota mínima (0.0 – 5.0)

### Tolerancia a errores

Se usa `fuzziness: AUTO` para tolerar errores tipográficos (ej:
"misteroi" encuentra "misterio").

## Campos indexados en Elasticsearch

| Campo             | Tipo ES   | Descripción                                |
|-------------------|-----------|--------------------------------------------|
| `Name`            | text      | Título del libro                           |
| `Authors`         | text      | Autor(es)                                  |
| `Description`     | text      | Descripción / sinopsis                     |
| `searchable_text` | text      | Concatenación: título+autores+edit.+desc.  |
| `Publisher`       | text      | Editorial                                  |
| `Language`        | keyword   | Código de idioma                           |
| `ISBN`            | keyword   | ISBN                                       |
| `Rating`          | float     | Nota promedio (0-5)                        |
| `PublishYear`     | integer   | Año de publicación                         |
| `CountsOfReview`  | integer   | Número de reseñas                          |
| `pagesNumber`     | integer   | Número de páginas                          |
| `RatingDist1..5`  | integer   | Distribución de calificaciones             |
| `RatingDistTotal` | integer   | Total de calificaciones                    |


