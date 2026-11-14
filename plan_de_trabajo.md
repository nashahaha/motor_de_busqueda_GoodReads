# Plan de Trabajo — Motor de Búsqueda Semántica de Libros de Goodreads

**CC5212-1 (2026-2) · Grupo 8 · Procesamiento Masivo de Datos**
Fecha de entrega: (completar) · Repositorio: https://github.com/nashahaha/motor_de_busqueda_GoodReads

---

## 1. Descripción del proyecto

- **Datos:** Dataset "Goodreads Book Datasets With User Rating 10M" (Kaggle). Dos tipos de
  archivos en el servidor:
  - `book*.csv` — libros (título, autores, descripción, etc.). **Son los que usamos.**
  - `user_rating_*.csv` — calificaciones de usuarios individuales. **No los usamos.**
  - Solo los archivos desde `book600k-700k.csv` en adelante (**17 archivos**) tienen la columna
    `Description`. Los 6 anteriores (`book1-100k` … `book500k-600k`) no la tienen y se excluyen.
  - Patrón de lectura estricto: `book*.csv`, nunca `*.csv`.
- **Tecnología:** Apache Spark para limpiar y normalizar los datos; Elasticsearch 6.8.10 como
  motor de búsqueda e indexación de texto; HDFS como sistema de archivos distribuido. Desarrollo
  según los laboratorios del curso (labs 3, 5, 7 y 8 como referencia).
- **Objetivo principal:** Construir un motor de búsqueda sobre Elasticsearch que permita recuperar
  libros de interés no solo por coincidencia en título o autor, sino también por palabras clave
  presentes en la descripción (p.ej. "misterio", "superación personal", "viaje espacial").
- **Confirmación del profesor (propuesta aprobada):** Spark + Elasticsearch alineado con los
  contenidos de las Classes 7 y 8; la escala es adecuada.

**Descubrimiento clave (Fase 0):** el "10M" del nombre del dataset corresponde a las ~10M
calificaciones (`user_rating_*.csv`), no a los libros. La escala medible y usable son
**1.512.190 libros** (17 archivos con `Description`, ≈1.06 GB en CSV crudo, 646.3 MB en parquet).
Sigue siendo comparable o superior al lab 7 (964.838 artículos indexados), ahora con descripciones
completas y mucho más largas.

## 2. Entorno, datos y metodología

- Acceso al clúster por SSH (`cm.dcc.uchile.cl`, puerto 220, usuario `uhadoop`). Servidor y HDFS
  **compartidos** por todo el curso: solo escribir/borrar dentro de
  `/data/2026/uhadoop/projects/grupo8/` (LFS del master) y `/uhadoop2026/grupo8/` (DFS).
- Spark vía `spark-submit`. Para trabajos completos usar siempre `--master spark://cluster-01:7077`
  (el modo local sobrecarga a la máquina maestra y agota la memoria; verificado en Fase 1).
- Elasticsearch: API REST en `cm:9200` (CLI con `curl`); el índice del proyecto será `goodreads-books`.
- Buenas prácticas seguidas: (a) muestra pequeña primero (`sample_books`, 5.000 filas) antes de
  cualquier corrida completa; (b) trabajos largos dentro de `screen`; (c) resultados y evidencias
  guardadas como archivos en `results/`; (d) un slice de desarrollo = una verificación manual
  ("checkpoint") antes de avanzar; no se avanza con un checkpoint en rojo.
- Metodología: **slices verticales con verificación manual** (variante ligera de FDD / TDD manual).
  Muestra → validación → corrida completa en el clúster. Sin tests unitarios automatizados.

## 3. Fases del plan

### Fase 0 — Exploración y unión · (COMPLETADA)

**Qué se hizo:**

1. Inventario HDFS de `/uhadoop2026/grupo8/`: 23 `book*.csv`, confirma que los 17 desde
   `book600k-700k.csv` en adelante tienen `Description` (18 columnas los 6 primeros; 19 o 20 los demás
   — 10 de ellos traen la columna extra `Count of text reviews`).
2. `explorar.py` ajustado al patrón estricto `/book[^/]*\.csv$` (nunca `*.csv`) y ejecución en
   el clúster; `unir.py` corrige además el problema de que los archivos pueden traer columnas en
   orden variable (p.ej. `RatingDist*` en orden 5→1) y `pagesNumber`/`PagesNumber`.
3. `hdfs:///uhadoop2026/grupo8/raw_union` generado (parquet): **1.512.190 filas**.
4. `sample_books` (5.000 filas) creado para iterar rápido.
5. **Formato real observado:** `RatingDist1..5` = `"<bucket>:<conteo>"` (p.ej. `"5:10"`),
   `RatingDistTotal` = `"total:62"`; `Language` con `"null"` literal como placeholder de faltante
   (luego se confirmó que son mayoritariamente NULLs reales); descripciones multiidioma (alemán,
   francés, inglés, español…).

**Verificación Fase 0 (checkpoint cumplido):** `raw_union.count()` = 1.512.190; muestra revisada;
evidencia guardada (inventario, exploración y conteo documentados en `results/`).

### Fase 1 — Limpieza y normalización con Spark · (COMPLETADA)

Script `limpiar.py`. Entrada `raw_union` → salida `clean_books` (parquet) + `clean_books_json` (JSONL).

1. Campos finales: `Id`, `Name`, `Authors`, `Description`, `Language`, `Publisher`, `PublishYear`,
   `Rating`, `RatingDist1..5`, `RatingDistTotal`, `CountsOfReview`.
2. Normalizaciones aplicadas (validadas sobre muestra y confirmadas en corrida completa):
   - `PublishYear` → entero; <1000 o > año actual ⇒ null; textos "####"/"null"/"" ⇒ null.
   - `Rating` → float 0–5; contadores ⇒ enteros no negativos (`RatingDist*` tomando la parte
     tras `:` del formato real `"<bucket>:<conteo>"`/`"total:62"`).
   - `Description`: unescape de entidades HTML, eliminación de etiquetas, colapso de
     whitespace/newlines (requerido para exportar JSONL con una línea por documento).
   - `Language`: minúsculas/trim; vacío y placeholder `"null"` y NULLs reales ⇒ `unknown`.
   - `Authors`: colapso de espacios; filas sin `Name` descartadas (hubo 0); `Id` deduplicado (hubo 0).
3. **Resultados medidos:** 1.512.190 filas finales; **341.080 (22.56 %) descripciones
   vacías/nulas**; **1.337.666 (~88 %) `unknown`** en idioma; `eng` 120.368, `fre` 14.215, `en-us`
   12.284, `ger` 10.356, `spa` 5.379, `jpn` 1.913.
4. **Lección de rendimiento:** la corrida en modo local agotó el heap (`OutOfMemoryError`);
   corregido ejecutando con `--master spark://cluster-01:7077` y sin `persist` (las estadísticas se
   calculan en una sola agregación); verificado el JSONL: **1.512.190 líneas = 1.512.190 documentos**.

**Verificación Fase 1 (checkpoint cumplido):** conteos antes/después documentados; % descripciones
vacías y distribución de idioma medidos y guardados en `results/`.

### Fase 2 — Diseño del índice Elasticsearch · (SIGUIENTE)

Índice `goodreads-books` (minúsculas, tipo `_doc`), creado con `curl -X PUT cm:9200/...`.
Mapping explícito: `NAME`, `AUTHORS`, `DESCRIPTION` como `text` con analyzer (candidato: `spanish`,
como en el lab 7; se evaluará `standard`/`english` sobre la muestra — las descripciones son
mayormente en inglés); `LANGUAGE`, `PUBLISHER`, `ID` como `keyword`; `PUBLISH_YEAR` y `REVIEWS`
como `integer`; `RATING` como `float`; `RATING_DIST` (o 5 campos `integer` + `total`).

- Piloto: indexar ~100 documentos de la muestra y verificar `_search` con las consultas objetivo
  ("misterio", "superación personal", "viaje espacial").
- Decisiones a cerrar en esta fase: tamaño del índice con/sin `store:true` en `DESCRIPTION`;
  boosts iniciales `NAME^5`, `AUTHORS^2`, `DESCRIPTION^1` (lab 7 usa `TITLE^5`/`ABSTRACT^1`).

**Verificación Fase 2:** índice creado con mapping esperado; piloto respondiendo en `_search`;
comandos y salidas guardados en `results/`.

### Fase 3 — Carga masiva (bulk indexing) · Lab 7 (BuildWikiIndexBulk)

Script `indexar.py` (Python + elasticsearch-py 6.x, compatible con ES 6.8.10 y tipo `_doc`).
Lectura streaming del JSONL/parquet, bulk por lotes de ~250–1.000 documentos, progreso cada 10.000,
errores por documento contados y logueados (la indexación continúa). Primero sobre la muestra;
luego completa en `screen` con tiempos medidos. Si el tiempo total supera lo razonable, evaluar
subset justificado (p.ej. solo libros con descripción) y documentar el sesgo.

**Verificación Fase 3:** `_count` en ES == `clean_books.count()`; tiempos de indexación y tamaño
del índice guardados en `results/`.

### Fase 4 — Motor de búsqueda (CLI) · Lab 7 (SearchWikiIndex)

Script `buscar.py`: CLI interactiva (`DOCS_PER_PAGE=10`). Consulta `multi_match` sobre
`NAME^5`, `AUTHORS^2`, `DESCRIPTION^1`; filtros opcionales `--lang`, `--desde-año`, `--hasta-año`,
`--min-rating`, `--min-reviews`; salida con título, autores, rating, año, idioma, snippet de
descripción y score. Boost adicional por popularidad (flag `--con-boost`, análogo al uso de
PageRank del lab 8) combinando score de ES con rating/reseñas. Resultados de ejemplo guardados en
`results/`.

**Verificación Fase 4:** las 3 consultas objetivo devuelven top-10 razonables; comparación
con/sin boost documentada; latencia por consulta anotada.

### Fase 5 — Evaluación · criterios Escala / Dificultad / Creatividad

- 10–15 consultas temáticas en español (misterio, viaje espacial, superación personal, romance
  histórico, ciencia ficción, cocina, terror, negocios…).
- Comparaciones: baseline (solo NAME/AUTHORS) vs `multi_match` completo vs boosts; top-10 por
  consulta con juicio cualitativo (relevante / parcial / no relevante).
- Métricas de sistema: tiempo total de indexación, tamaño del índice (`_cat/indices`), latencia
  p50/p95, documentos indexados, % con descripción.
- Estadísticas de datos para el informe (Spark): distribución de idioma/año/rating.

**Verificación Fase 5:** `results/` con consultas, tablas comparativas y métricas; tablas/figuras
para el informe.

### Fase 6 — Entregables y cierre

- Repositorio limpio: `.gitignore`, `README.md` con pasos de reproducción (HDFS → `limpiar.py` →
  `indexar.py` → `buscar.py`).
- **Informe** (LaTeX, 5–6 páginas): problema, datos, pipeline, diseño del índice, decisiones,
  resultados de Fase 5, limitaciones.
- **Presentación** (PDF, máx. 8 minutos).
- **Evaluación de pares** de los 3 proyectos asignados.
- **Limpieza final:** borrar intermedios propios en HDFS/LFS; no tocar datos de otros grupos.

## 4. Decisiones y hallazgos clave (Fases 0–1)

- Solo los **17** `book*.csv` con `Description` se usan; `user_rating_*.csv` fuera de alcance.
- Escala real: **1.512.190 libros** (el "10M" es de calificaciones); sigue superando al lab 7.
- **`Language` mayoritariamente faltante (~88 %)**: se normaliza a `unknown` de forma uniforme
  (NULLs y placeholder `"null"`) para que los filtros y facetas del índice sean consistentes.
- **22.56 %** de descripciones vacías tras limpiar; las filas se conservan (permite medir el
  impacto de la búsqueda por descripción).
- Formato de `RatingDist*`: `"<bucket>:<conteo>"` con `total:N` en `RatingDistTotal`.
- Export a Elasticsearch vía JSONL (`df.write.json`): cada línea es un documento; el pipeline
  completo está en HDFS (`raw_union` 646.3 MB → `clean_books` + `clean_books_json`).
- Rendimiento: `--master spark://cluster-01:7077` obligatorio para corridas completas;
  `persist` evitado (OOM); estadísticas en una única agregación.
- Índice ES será `goodreads-books` (REST `cm:9200`, ES 6.8.10, tipo `_doc`); cliente Python
  elasticsearch-py 6.x siguiendo el patrón del lab 7.
