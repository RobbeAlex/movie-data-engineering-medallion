# Movie Data Engineering Medallion: Pipeline de 5 Etapas y Clasificación Cinematográfica 🎬📊

Pipeline End-to-End de Ingeniería de Datos y Machine Learning estructurado bajo la **Arquitectura Medallion (Raw → Bronze → Silver → Gold → Model/Application)** para el procesamiento distribuido simulado de 100,000 registros de películas globales (`1950–2026`), gobernanza de datos (CDC, auditoría y linaje) y entrenamiento supervisado con **Random Forest**.

---

## 🏗️ Arquitectura de las 5 Etapas del Pipeline

El flujo de trabajo se divide en 5 Jupyter Notebooks secuenciales respaldados por el módulo transversal `src/utils.py`:

1. **`src/01_ingesta.ipynb` (Capa Raw - Ingesta Distribuida)**:
   - Lee el dataset maestro `datasets/global_movies_dataset_1950_2026.csv` (100,000 películas).
   - Ordena cronológicamente por `release_year` y particiona el dataset en 5 fragmentos equitativos (20,000 registros c/u) usando `numpy.array_split`, simulando múltiples nodos de ingesta.
   - Persiste las tablas `raw_fuente_1` a `raw_fuente_5` en `database/raw_database.db` y registra la auditoría en `results/extraction_log.json`.

2. **`src/02_extracted.ipynb` (Capa Bronze - Extracción y Metadatos CDC)**:
   - Extrae las 5 particiones desde `raw_database.db` y enriquece cada registro con metadatos de trazabilidad: identificador único `metadata_uuid` (UUIDv4), marca temporal `metadata_extraction_date` e identificador de origen `metadata_source`.
   - Almacena las tablas `extracted_fuente_1` a `extracted_fuente_5` en `database/extracted_database.db`.
   - Actualiza el estado de captura de datos modificados (*Change Data Capture*) en `results/cdc_control.json`.

3. **`src/03_transformed.ipynb` (Capa Silver - Limpieza y Estandarización)**:
   - Consolida las 5 fuentes de la capa Bronze, normaliza los encabezados de columna (`lowercase` + `strip`) y aplica reglas de calidad e imputación de valores faltantes (mediana para atributos numéricos y `"unknown"` para categóricos).
   - Re-particiona y guarda las tablas limpias `transformed_fuente_1` a `transformed_fuente_5` en `database/transformed_database.db`.
   - Documenta las operaciones realizadas en `results/transformation_log.json`.

4. **`src/04_serving.ipynb` (Capa Gold - Modelo Estrella y Gobernanza)**:
   - Invoca `utils.create_star_schema()` para unificar las particiones limpias de la capa Silver en la tabla analítica de hechos `serving_fact_movies` (100,000 filas) dentro de `database/serving_database.db`.
   - Genera el contrato y diccionario de datos en `config/schema_design.json` y registra la trazabilidad completa en `results/lineage.json`.

5. **`src/05_aplicacion.ipynb` (Capa de Aplicación y Machine Learning)**:
   - Consume la tabla `serving_fact_movies` mediante `utils.entrenar_modelo_retail()`.
   - Aplica codificación *One-Hot* (`pd.get_dummies`), filtra columnas de ruido/metadatos (`id`, `uuid`, `metadata`, `title`, `overview`, `release_year`) y entrena un clasificador `RandomForestClassifier` (`n_estimators=100`, `random_state=42`) para predecir el éxito de taquilla (`blockbuster_flag`).
   - Serializa el modelo entrenado en `models/modelo_entrenado_seleccionado.pkl` y exporta las métricas (`Accuracy`, `Recall`, `Rows_Processed`) en `results/metricas.csv`.

---

## 📂 Estructura del Proyecto

```text
movie-data-engineering-medallion/
├── datasets/
│   └── global_movies_dataset_1950_2026.csv   # Dataset fuente (100,000 registros)
├── database/
│   ├── raw_database.db                       # SQLite Capa Raw (raw_fuente_1..5)
│   ├── extracted_database.db                 # SQLite Capa Bronze (extracted_fuente_1..5)
│   ├── transformed_database.db               # SQLite Capa Silver (transformed_fuente_1..5)
│   └── serving_database.db                   # SQLite Capa Gold (serving_fact_movies)
├── results/
│   ├── extraction_log.json                   # Auditoría de ingesta inicial
│   ├── cdc_control.json                      # Control de sincronización Change Data Capture
│   ├── transformation_log.json               # Registro de reglas de limpieza e imputación
│   └── lineage.json                          # Trazabilidad de linaje de datos
├── serving/
│   └── schema_design.json                    # Contrato de datos y esquema estrella
├── models/
│   ├── modelo_entrenado_seleccionado.pkl     # Modelo RandomForestClassifier serializado
│   └── metricas.csv                          # Métricas de evaluación (Accuracy: 0.9979, Recall: 0.9948)
├── 01_ingesta.ipynb                          # Etapa 1: Ingesta a Capa Raw
├── 02_extracted.ipynb                        # Etapa 2: Metadatos y CDC a Capa Bronze
├── 03_transformed.ipynb                      # Etapa 3: Limpieza e imputación a Capa Silver
├── 04_serving.ipynb                          # Etapa 4: Unificación en Esquema Estrella (Capa Gold)
├── 05_aplicacion.ipynb                       # Etapa 5: Entrenamiento y serialización ML
├── utils.py                                  # Módulo central de rutas dinámicas, SQLite, logs y ML
├── Reporte.pdf                               # Reporte ejecutivo del proyecto integrador
├── DOCUMENTACION_TECNICA.md                  # Especificación técnica detallada y diagrama de flujo
├── requirements.txt                          # Dependencias de Python
├── LICENSE                                   # Licencia MIT
└── .gitignore                                # Exclusiones de Git
```

---

## ⚙️ Instalación y Uso

1. **Crear y activar entorno virtual**:
   ```bash
   python -m venv .venv
   # En Windows:
   .venv\Scripts\activate
   ```

2. **Instalar dependencias**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Ejecutar el Pipeline**:
   Ejecutar los notebooks en orden secuencial del `src/01_ingesta.ipynb` al `src/05_aplicacion.ipynb`. Todas las rutas de bases de datos, logs y modelos se resuelven dinámicamente a través de `src/utils.py` usando `os.path.dirname(os.path.abspath(__file__))`.

---

## 📄 Licencia

Distribuido bajo la licencia MIT. Consulta el archivo `LICENSE` para más información.
