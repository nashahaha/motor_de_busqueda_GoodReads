# AGENTS.md — Guía para agentes de IA

Instrucciones para agentes (y humanos) que trabajen en este repositorio.
Documento en español, como el resto del proyecto.

## 1. Contexto mínimo del proyecto

- **Qué es:** motor de búsqueda semántica de libros del dataset *Goodreads Book Datasets With
  User Rating 10M* (Kaggle). Permite recuperar libros por palabras clave presentes en la
  **descripción** (p.ej. "misterio", "superación personal", "viaje espacial"), no solo por
  título o autor.
- **Stack:** Apache Spark (limpieza y normalización de datos) + Elasticsearch 6.8.10 (indexación
  y búsqueda, API REST en `cm:9200`) + HDFS como sistema de archivos. Desarrollo según los
  laboratorios del curso (labs 3, 5, 7 y 8 como referencia).
- **Datos:** solo archivos `book*.csv` con columna `Description` — es decir los **17** desde
  `book600k-700k.csv` en adelante. Los 6 anteriores no tienen `Description` y `user_rating_*.csv`
  no se usan. Patrón de lectura estricto: `book*.csv`, nunca `*.csv`.
- **Rutas:** datos en HDFS `/uhadoop2026/grupo8/`; LFS del master `/data/2026/uhadoop/projects/grupo8/`;
  salida ES como índice `goodreads-books`. Clúster compartido (ver §5.3).
- **Escala:** ~10M registros (aprobado por el profesor). Repo local: `motor_de_busqueda_GoodReads`;
  remoto compartido con la compañera: `github.com/nashahaha/motor_de_busqueda_GoodReads`.
  **El repo no debe contener datos ni información sensible.**

## 2. Documentación del proyecto

`context/` está **excluido del git** (material del curso y trabajo local); material definitivo
irá a futuro a `docs/`.

| Documento | Contenido |
|-----------|-----------|
| `context/tema.md` | Enunciado del tema y consejos del profesor |
| `context/plan_trabajo.md` | Alcance, entregables, productos y rúbrica, riesgos |
| `context/implementation_plan.md` | Fases 0–6 detalladas en slices con **checkpoints manuales** (metodología de implementación) |
| `context/informe/agent_inform.md` | **Bitácora obligatoria** con estilo de informe (ver §4) |
| `context/labs/` | PDFs y códigos base de los laboratorios (referencia, no redistribuir) |

Los documentos `plan_trabajo.md` e `implementation_plan.md` son la fuente de verdad del alcance.
Si durante el desarrollo cambian decisiones, actualizarlos antes de seguir.

## 3. Metodología de implementación

Se implementa siguiendo `context/implementation_plan.md`: **slices verticales con checkpoints
manuales** (variante ligera de FDD / TDD manual).

- Cada slice encapsula un incremento pequeño y termina en un checkpoint manual con criterio
  pre-definido (qué comprobar y qué se espera). **No se avanza a la fase siguiente con un
  checkpoint en rojo.**
- Siempre **muestra pequeña primero** (`sample_books`, ~1.000–10.000 filas) antes de correr sobre
  los datos completos; nunca validar reglas directamente con los 17 archivos.
- Trabajos largos en el clúster dentro de `screen` (`screen -d -r NUM` para retomar).
- Un slice ≈ un commit; revisar `git status`/`diff` antes de cada commit.
- Evidencia de cada checkpoint → `results/` (salidas, conteos, tablas).
- Al cerrar cada slice o fase, **anotar la bitácora** (§4) antes de continuar.

## 4. Bitácora obligatoria: `context/informe/agent_inform.md`

Todo descubrimiento, hallazgo técnico, incidencia, decisión y resultado de la implementación debe
quedar anotado en `context/informe/agent_inform.md`, con **estilo de informe** y secciones
numeradas:

1. Introducción · 2. Metodología e implementación por fase · 3. Resultados y evidencia ·
4. Decisiones técnicas · 5. Incidencias · 6. Conclusiones

Reglas:

- Portada breve (autores, curso, fecha de inicio, estado) e índice al inicio.
- Una entrada fechada por slice/fase con: qué se hizo, cómo se verificó el checkpoint, hallazgos,
  comandos relevantes y referencias a `results/`.
- Toda cifra del informe debe provenir de `results/`; no inventar ni redondear sin fuente.
- Redactar en español, tono formal pero conciso; sin emojis.
- Actualizar la sección 3 (Resultados) conforme avance Fase 5.

## 5. Protecciones mínimas

### 5.1 Credenciales

- **No entregadas aún.** Cuando el usuario las proporcione a futuro (acceso SSH a
  `cm.dcc.uchile.cl`, puerto 220, usuario `uhadoop`):
  - Jamás en archivos del repo, notas, bitácora, commits, ni comandos versionados.
  - Jamás hardcodeadas; no pedirlas, imprimirlas ni transcribirlas; no pegarlas en URLs.
  - Si se exponen por accidente (log, commit, salida de comando): **avisar de inmediato** al
    usuario y detener el trabajo; la contraseña debe rotarse.
  - Cuenta **compartida del curso**: no reutilizar contraseñas personales; no usarla para
    propósitos ajenos al proyecto.
- No crear ni subir archivos con secretos (`.env` está en `.gitignore`; aun así, preferir no
  escribir secretos en disco).

### 5.2 Repositorio

- `context/` (material del curso, bitácora, planes) y `.idea/` **no se versionan** (ver `.gitignore`).
- No commitear: datasets, `clean_books.jsonl`, parquet, zips, PDFs/código de los laboratorios.
- `results/`: solo evidencias livianas (`.txt`, `.md`); no binarios ni exportaciones grandes.
- Revisar siempre `git status` y `git diff` antes de commit; nunca `git commit -a` a ciegas.

### 5.3 Clúster compartido

- El servidor (`cluster-01`) y HDFS son **compartidos** (todos usan el mismo usuario). Solo se puede
  acceder y modificar contenido dentro de:
  - **LFS (sistema de archivos local) del master:** `/data/2026/uhadoop/projects/grupo8/`
  - **DFS (HDFS):** `/uhadoop2026/grupo8/`
  - Está **estrictamente prohibido** trabajar fuera de estas carpetas (ni leer, editar, ni borrar).
- Nunca `rm -r`, `hdfs dfs -rm -r` ni `kill` fuera de esas carpetas o a jobs ajenos.
- Trabajos largos con `screen`; verificar el resultado al retomar (nunca asumir éxito).
- Borrar intermedios propios ya superados (consejo del profesor); no tocar datos de otros grupos.
- Correr en horas de baja carga cuando sea posible; el clúster es compartido por todo el curso.

### 5.4 Elasticsearch

- Índices propios: `goodreads-books` y `goodreads-books-test`. No crear, modificar ni eliminar
  índices ajenos de otros grupos o del curso.
- Al terminar pruebas, eliminar `goodreads-books-test`; medir y documentar antes de borrar nada.
- `curl "cm:9200/_cat/indices?v"` para verificar el estado antes de cualquier operación masiva.

### 5.5 Datos

- El dataset es de Kaggle (*Goodreads Book Datasets*): no redistribuir el crudo por el repo ni
  re-subirlo sin necesidad; mantener réplica 1 en HDFS para intermedios; borrar locales tras copiar.

## 6. Comandos frecuentes

```bash
ssh -p 220 uhadoop@cm.dcc.uchile.cl      # acceso al master del clúster (credenciales aparte)
hdfs dfs -ls -h /uhadoop2026/grupo8/     # ver archivos en DFS
hdfs dfs -du -h /uhadoop2026/grupo8/     # tamaños
spark-submit explorar.py 2>/dev/null      # ejecutar trabajo Spark
screen                                    # sesión persistente (Ctrl+A, D para detach; screen -d -r NUM)
curl "cm:9200/_cat/health?v&pretty"       # estado de Elasticsearch
curl "cm:9200/_cat/indices?v"             # índices existentes
curl "cm:9200/goodreads-books/_count"     # conteo de documentos
```

## 7. Convenciones de trabajo

- Español en documentación y comentarios; sin emojis.
- Scripts planos como los labs (`explorar.py`, `unir.py`, `limpiar.py`, `indexar.py`, `buscar.py`),
  con `if __name__ == "__main__":`; resultados y evidencias en `results/`.
- No crear documentos nuevos sin que el usuario lo pida; los existentes se actualizan en su lugar.
- Pedir confirmación antes de cualquier operación destructiva (HDFS, ES, git).
- Si una decisión cambia, anotarla en la bitácora (§4) y en el plan que corresponda.
