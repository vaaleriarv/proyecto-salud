import pandas as pd
import numpy as np
import sqlite3

# Conexión a la base de datos
conn = sqlite3.connect("pipeline.db")
df = pd.read_sql("SELECT * FROM BRFSS_2024", conn)
print(f"Tabla BRFSS_2024 cargada: {df.shape[0]:,} filas x {df.shape[1]} columnas")
print("Columnas disponibles:")
for col in df.columns:
    print(f"  - {col}")

def mapear_columna(df, col, mapa, reemplazar_nones=True):
    if col in df.columns:
        antes = df[col].notna().sum()
        df[col] = df[col].apply(lambda x: mapa.get(int(x)) if pd.notna(x) and str(x).isdigit() else np.nan)
        if reemplazar_nones:
            df[col] = df[col].replace(['No sabe / No respondió', 'None'], np.nan)
        despues = df[col].notna().sum()
        print(f"Procesando columna '{col}'...")
        print(f"  Valores válidos antes: {antes:,}")
        print(f"  Valores válidos después: {despues:,}")
    else:
        print(f"Columna '{col}' no encontrada y se omite")

# 1. _STATE
estado_map = {
    1: "Alabama", 2: "Alaska", 4: "Arizona", 5: "Arkansas", 6: "California",
    8: "Colorado", 9: "Connecticut", 10: "Delaware", 11: "Distrito de Columbia",
    12: "Florida", 13: "Georgia", 15: "Hawái", 16: "Idaho", 17: "Illinois",
    18: "Indiana", 19: "Iowa", 20: "Kansas", 21: "Kentucky", 22: "Louisiana",
    23: "Maine", 24: "Maryland", 25: "Massachusetts", 26: "Michigan",
    27: "Minnesota", 28: "Mississippi", 29: "Missouri", 30: "Montana",
    31: "Nebraska", 32: "Nevada", 33: "New Hampshire", 34: "New Jersey",
    35: "New Mexico", 36: "New York", 37: "North Carolina", 38: "North Dakota",
    39: "Ohio", 40: "Oklahoma", 41: "Oregon", 42: "Pennsylvania",
    44: "Rhode Island", 45: "South Carolina", 46: "South Dakota",
    47: "Tennessee", 48: "Texas", 49: "Utah", 50: "Vermont",
    51: "Virginia", 53: "Washington", 54: "West Virginia", 55: "Wisconsin",
    56: "Wyoming"
}
mapear_columna(df, "_STATE", estado_map)

# 2. ESTADO CIVIL
marital_map_es = {
    1: 'Casado/a', 2: 'Divorciado/a', 3: 'Viudo/a', 4: 'Separado/a',
    5: 'Nunca se ha casado', 6: 'Pareja no casada', 9: 'No sabe / No respondió'
}
mapear_columna(df, "MARITAL", marital_map_es)

# 3. _EDUCAG
educag_map = {
    1: 'No completó secundaria', 2: 'Graduado de secundaria',
    3: 'Asistió a universidad o escuela técnica',
    4: 'Graduado de universidad o escuela técnica',
    9: 'No sabe / Falta de información'
}
mapear_columna(df, "_EDUCAG", educag_map)

# 4. _CHLDCNT
children_map_es = {
    1: 'Sin niños en el hogar', 2: '1 niño en el hogar', 3: '2 niños en el hogar',
    4: '3 niños en el hogar', 5: '4 niños en el hogar', 6: '5 o más niños en el hogar',
    9: 'No sabe / No respondió'
}
mapear_columna(df, "_CHLDCNT", children_map_es)

# 5. _INCOMG1
income_map_es = {
    1: 'Menos de $15,000', 2: '$15,000 a < $25,000', 3: '$25,000 a < $35,000',
    4: '$35,000 a < $50,000', 5: '$50,000 a < $100,000',
    6: '$100,000 a < $200,000', 7: '$200,000 o más', 9: 'No sabe / No respondió'
}
mapear_columna(df, "_INCOMG1", income_map_es)

# 6. _AGE_G
age_g_map = {1: '18-24', 2: '25-34', 3: '35-44', 4: '45-54', 5: '55-64', 6: '65+'}
mapear_columna(df, "_AGE_G", age_g_map)

# 7. _SEX
sex_map = {1: 'Hombre', 2: 'Mujer'}
mapear_columna(df, "_SEX", sex_map)

# 8. _URBSTAT
urbstat_map = {1: 'Urbano', 2: 'Rural'}
mapear_columna(df, "_URBSTAT", urbstat_map)


# 9. _METSTAT
metstat_map = {1: 'Condados metropolitanos', 2: 'Condados no metropolitanos'}
mapear_columna(df, "_METSTAT", metstat_map)

#
# 10. MEDCOST1
medcost_map = {1: 'Sí, no pudo pagar', 2: 'No', 7: 'No sabe / Incertidumbre', 9: 'Se negó a responder'}
mapear_columna(df, "MEDCOST1", medcost_map)


checkup_map = {
    1: 'En el último año (<12 meses)', 2: 'Hace 1 a 2 años', 3: 'Hace 2 a 5 años',
    4: 'Hace 5 o más años', 7: 'No sabe / Incertidumbre', 8: 'Nunca', 9: 'Se negó a responder'
}
mapear_columna(df, "CHECKUP1", checkup_map)

print("\n✔ Tabla 'BRFSS_2024_LIMPIO' creada correctamente con los valores mapeados.\n")
print("Primeras 5 filas de la tabla limpia:")
print(df.head())

print("\nResumen de completitud por columna:")
calidad = pd.DataFrame({
    'Columna': df.columns,
    'Tipo': df.dtypes.values,
    'No Nulos': df.count().values,
    'Nulos': df.isnull().sum().values,
    'Pct Completo': (df.count() / len(df) * 100).round(2).values
})
calidad = calidad.sort_values('Pct Completo', ascending=False)
print(calidad.to_string(index=False))

df.to_sql("BRFSS_2024_LIMPIO", conn, if_exists="replace", index=False)

# Crear índices para facilitar consultas
cursor = conn.cursor()
indices = [
    "CREATE INDEX IF NOT EXISTS idx_brfss_estado ON BRFSS_2024_LIMPIO(_STATE);",
    "CREATE INDEX IF NOT EXISTS idx_brfss_edad ON BRFSS_2024_LIMPIO(_AGE_G);",
    "CREATE INDEX IF NOT EXISTS idx_brfss_sexo ON BRFSS_2024_LIMPIO(_SEX);"
]
for idx_query in indices:
    try:
        cursor.execute(idx_query)
    except Exception as e:
        print(f"Error creando índice: {e}")

conn.commit()
conn.close()
print("\nLimpieza y almacenamiento completados")
