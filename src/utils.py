import os
import sqlite3
import pandas as pd
import numpy as np
import json
import joblib
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score

# ==========================================
# 1. CENTRALIZACIÓN DE RUTAS (CONFIGURACIÓN)
# ==========================================
PROYECTO_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Carpetas Principales
DATASETS_PATH = os.path.join(PROYECTO_BASE, "datasets")
DB_PATH = os.path.join(PROYECTO_BASE, "database")
LOGS_PATH = os.path.join(PROYECTO_BASE, "results")
MODELS_PATH = os.path.join(PROYECTO_BASE, "results")
SERVING_PATH = os.path.join(PROYECTO_BASE, "config")

# Bases de Datos (Capas Medallion)
RAW_DB_PATH = os.path.join(DB_PATH, "raw_database.db")
EXTRACTED_DB_PATH = os.path.join(DB_PATH, "extracted_database.db")
TRANSFORMED_DB_PATH = os.path.join(DB_PATH, "transformed_database.db")
SERVING_DB_PATH = os.path.join(DB_PATH, "serving_database.db")

# Archivos de Salida Específicos (Solicitados)
TRANSFORM_LOG_PATH = os.path.join(LOGS_PATH, "transformation_log.json")
LINEAGE_PATH = os.path.join(LOGS_PATH, "lineage.json")
MODEL_PATH = os.path.join(MODELS_PATH, "modelo_entrenado_seleccionado.pkl")
METRICS_PATH = os.path.join(LOGS_PATH, "metricas.csv")
SCHEMA_DESIGN_PATH = os.path.join(SERVING_PATH, "schema_design.json")

# Otros
CDC_PATH = os.path.join(LOGS_PATH, "cdc_control.json")
LOG_PATH = os.path.join(LOGS_PATH, "extraction_log.json")

# Asegurar que las carpetas existan
for carpeta in [DB_PATH, LOGS_PATH, MODELS_PATH, SERVING_PATH]:
    os.makedirs(carpeta, exist_ok=True)


# ==========================================
# 2. FUNCIONES DE CONEXIÓN Y UTILIDAD
# ==========================================
def get_db_connection(layer):
    """Retorna una conexión a la base de datos de la capa especificada."""
    paths = {
        "raw": RAW_DB_PATH,
        "extracted": EXTRACTED_DB_PATH,
        "transformed": TRANSFORMED_DB_PATH,
        "serving": SERVING_DB_PATH
    }
    return sqlite3.connect(paths.get(layer, RAW_DB_PATH))


def get_timestamp():
    """Retorna la fecha y hora actual formateada."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def save_log(path, data):
    """Guarda un diccionario como archivo JSON en la ruta especificada."""
    with open(path, "w", encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


# ==========================================
# 3. FUNCIONES DE PROCESAMIENTO (PIPELINE)
# ==========================================

def create_star_schema():
    """
    Unifica los fragmentos de la capa Silver (Transformed) y los persiste
    en la capa Gold (Serving) bajo un diseño de Esquema Estrella.
    """
    # 1. Conexión a la capa de origen (Silver)
    conn_trans = get_db_connection("transformed")

    try:
        # Unificamos los 5 fragmentos generados en el Notebook 03
        fragmentos = []
        for i in range(1, 6):
            tabla_nombre = f"transformed_fuente_{i}"
            df_temp = pd.read_sql(f"SELECT * FROM {tabla_nombre}", conn_trans)
            fragmentos.append(df_temp)

        # Concatenación de los 20,000 registros
        df_unificado = pd.concat(fragmentos, ignore_index=True)
        print(f"✅ Datos unificados exitosamente. Total: {len(df_unificado)} filas.")

    except Exception as e:
        print(f"❌ Error al unificar fragmentos: {e}")
        return 0
    finally:
        conn_trans.close()

    # 2. Conexión a la capa de destino (Gold/Serving)
    conn_serving = get_db_connection("serving")

    try:
        # Persistencia en la tabla de hechos de películas
        # Se usa 'replace' para asegurar una carga limpia en cada ejecución
        df_unificado.to_sql("serving_fact_movies", conn_serving, if_exists="replace", index=False)

        total_final = len(df_unificado)
        print(f"✅ Tabla 'serving_fact_movies' creada con {total_final} registros.")
        return total_final

    except Exception as e:
        print(f"❌ Error al guardar en la capa Serving: {e}")
        return 0
    finally:
        conn_serving.close()


def entrenar_modelo_retail():
    """
    Notebook 05: Entrena el modelo y genera modelo_entrenado_seleccionado.pkl y metricas.csv.
    """
    conn = get_db_connection("serving")
    df = pd.read_sql("SELECT * FROM serving_fact_movies", conn)
    conn.close()

    # 1. Selección del objetivo de Kaggle
    # Puedes cambiar 'blockbuster_flag' por 'top_100_prob' si prefieres regresión
    target = 'blockbuster_flag'

    if target not in df.columns:
        raise KeyError(f"No se encontró {target}. Columnas: {df.columns.tolist()}")

    # 2. Limpieza y Encoding
    df_ml = pd.get_dummies(df)

    # Palabras ruido actualizadas para el dataset de 100k
    palabras_ruido = ['id', 'uuid', 'metadata', 'index', 'title', 'url', 'overview', 'release_year']
    cols_a_eliminar = [c for c in df_ml.columns if any(p in c.lower() for p in palabras_ruido) and c.lower() != target]

    df_ml = df_ml.drop(columns=cols_a_eliminar)

    # 3. Entrenamiento (Random Forest)
    from sklearn.ensemble import RandomForestClassifier  # Cambiar a Regressor si usas top_100_prob
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, recall_score

    X = df_ml.drop(columns=[target])
    y = df_ml[target]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Métricas
    preds = model.predict(X_test)
    metricas = {
        "Accuracy": accuracy_score(y_test, preds),
        "Recall": recall_score(y_test, preds),
        "Rows_Processed": len(df)
    }

    return model, metricas


