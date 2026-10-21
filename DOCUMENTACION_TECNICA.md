# Documentación Técnica: Movie Data Engineering Medallion (Pipeline de 5 Etapas)

**Autor:** Roberto Alejandro Chagra Martínez  
**Tecnologías:** Python, Jupyter Notebook, Pandas, NumPy, SQLite3, Scikit-Learn, Joblib  
**Tipo:** Pipeline End-to-End de Ingeniería de Datos (Medallion Architecture) y Machine Learning  

---

## 1. Descripción Arquitectónica

Este proyecto implementa un pipeline completo de Ingeniería de Datos sobre el dataset [`datasets/global_movies_dataset_1950_2026.csv`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/datasets/global_movies_dataset_1950_2026.csv) ($N = 100,000$ películas). El diseño desacopla el almacenamiento físico en cuatro bases de datos SQLite independientes dentro de [`database/`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/database) correspondientes a las capas **Raw**, **Bronze (Extracted)**, **Silver (Transformed)** y **Gold (Serving)**, manteniendo trazabilidad completa mediante bitácoras JSON en [`results/`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/logs) y contratos de esquema en [`serving/`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/serving).

---

## 2. Diagrama de Flujo del Pipeline

```mermaid
flowchart TB
    CSV["Dataset Fuente CSV\n(datasets/global_movies_dataset_1950_2026.csv)"] --> N1["01_ingesta.ipynb\nFragmentación en 5 fuentes"]
    N1 --> DB1[("raw_database.db\n(raw_fuente_1..5)")]
    N1 --> L1["results/extraction_log.json"]

    DB1 --> N2["02_extracted.ipynb\nInyección UUID + Timestamp + Origen"]
    N2 --> DB2[("extracted_database.db\n(extracted_fuente_1..5)")]
    N2 --> L2["results/cdc_control.json"]

    DB2 --> N3["03_transformed.ipynb\nNormalización e Imputación (Mediana/Unknown)"]
    N3 --> DB3[("transformed_database.db\n(transformed_fuente_1..5)")]
    N3 --> L3["results/transformation_log.json"]

    DB3 --> N4["04_serving.ipynb\nutils.create_star_schema()"]
    N4 --> DB4[("serving_database.db\n(serving_fact_movies - 100k filas)")]
    N4 --> S1["config/schema_design.json"]
    N4 --> L4["results/lineage.json"]

    DB4 --> N5["05_aplicacion.ipynb\nutils.entrenar_modelo_retail()"]
    N5 --> M1["models/modelo_entrenado_seleccionado.pkl\n(RandomForestClassifier)"]
    N5 --> M2["results/metricas.csv\n(Accuracy: 0.9979, Recall: 0.9948)"]
```

---

## 3. Especificación de Etapas y Módulos

### 3.1. Núcleo Transversal ([`src/utils.py`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/utils.py))
- **Gestión Dinámica de Rutas ([`utils.py#L15-L43`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/utils.py#L15-L43))**: Calcula `PROYECTO_BASE` dinámicamente y garantiza la existencia de los directorios [`database/`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/database), [`results/`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/logs), [`models/`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/models) y [`serving/`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/serving).
- [`get_db_connection(layer)`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/utils.py#L49-L57): Enrutador de conexiones SQLite hacia `raw_database.db`, `extracted_database.db`, `transformed_database.db` o `serving_database.db`.
- [`create_star_schema()`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/utils.py#L75-L117): Consolida las 5 tablas `transformed_fuente_1..5` de la capa Silver y construye la tabla `serving_fact_movies` en la capa Gold.
- [`entrenar_modelo_retail()`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/utils.py#L120-L165): Preprocesa `serving_fact_movies`, elimina atributos textuales/identificadores de alta cardinalidad y entrena un `RandomForestClassifier` sobre la variable objetivo `blockbuster_flag`.

### 3.2. Notebooks del Pipeline
1. **Etapa 1 — Ingesta ([`src/01_ingesta.ipynb`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/01_ingesta.ipynb))**: Ordena el dataset por año de lanzamiento (`release_year`), lo divide en 5 particiones de 20,000 registros y genera [`results/extraction_log.json`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/results/extraction_log.json).
2. **Etapa 2 — Extracción / Bronze ([`src/02_extracted.ipynb`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/02_extracted.ipynb))**: Añade `metadata_uuid`, `metadata_extraction_date` y `metadata_source` a cada lote y actualiza [`results/cdc_control.json`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/results/cdc_control.json).
3. **Etapa 3 — Transformación / Silver ([`src/03_transformed.ipynb`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/03_transformed.ipynb))**: Estandariza cabeceras e imputa valores nulos, registrando la auditoría en [`results/transformation_log.json`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/results/transformation_log.json).
4. **Etapa 4 — Serving / Gold ([`src/04_serving.ipynb`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/04_serving.ipynb))**: Materializa `serving_fact_movies`, exporta el diccionario de datos [`config/schema_design.json`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/config/schema_design.json) y persiste el linaje en [`results/lineage.json`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/results/lineage.json).
5. **Etapa 5 — Aplicación ML ([`src/05_aplicacion.ipynb`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/05_aplicacion.ipynb))**: Entrena el clasificador, guarda el artefacto [`models/modelo_entrenado_seleccionado.pkl`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/models/modelo_entrenado_seleccionado.pkl) y registra las métricas de validación en [`results/metricas.csv`](file:///g:/Mi%20unidad/Drive.alumno.udg.mx/Proyectos/movie-data-engineering-medallion/results/metricas.csv).
