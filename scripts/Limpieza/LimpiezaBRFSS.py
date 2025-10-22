import pandas as pd
import sqlite3

# --- Conexión a la base ---
conn = sqlite3.connect("pipeline.db")

df = pd.read_sql("SELECT * FROM BRFSS_2024", conn)
print("Tabla BRFSS_2024 cargada:", df.shape)

# MAPEOS PARA CAMBIAR VALORES NÚMERICOS DE LA ENCUESTA A SUS VALORES REALES

# --- 1. Estado ---
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
df["_STATE"] = df["_STATE"].astype(float).map(estado_map)

# --- 2. Estado civil ---
marital_map_es = {
    1: 'Casado/a',
    2: 'Divorciado/a',
    3: 'Viudo/a',
    4: 'Separado/a',
    5: 'Nunca se ha casado',
    6: 'Pareja no casada',
    9: 'Se negó a responder'
}
df["MARITAL"] = df["MARITAL"].apply(lambda x: marital_map_es.get(int(x)) if pd.notna(x) else x)

# --- 3. Nivel educativo ---
educag_map = {
    1: 'No completó secundaria',
    2: 'Graduado de secundaria',
    3: 'Asistió a universidad o escuela técnica',
    4: 'Graduado de universidad o escuela técnica',
    9: 'No sabe / Falta de información'
}
df["_EDUCAG"] = df["_EDUCAG"].apply(lambda x: educag_map.get(int(x)) if pd.notna(x) else x)

# --- 4. Niños en el hogar ---
children_map_es = {
    1: 'Sin niños en el hogar',
    2: '1 niño en el hogar',
    3: '2 niños en el hogar',
    4: '3 niños en el hogar',
    5: '4 niños en el hogar',
    6: '5 o más niños en el hogar',
    9: 'No sabe / No respondió'
}
df["_CHLDCNT"] = df["_CHLDCNT"].apply(lambda x: children_map_es.get(int(x)) if pd.notna(x) else x)

# --- 5. Ingreso ---
income_map_es = {
    1: 'Menos de $15,000',
    2: '$15,000 a < $25,000',
    3: '$25,000 a < $35,000',
    4: '$35,000 a < $50,000',
    5: '$50,000 a < $100,000',
    6: '$100,000 a < $200,000',
    7: '$200,000 o más',
    9: 'No sabe / No respondió'
}
df["_INCOMG1"] = df["_INCOMG1"].apply(lambda x: income_map_es.get(int(x)) if pd.notna(x) else x)

# --- 6. _AGE_G en rangos de edad ---
age_g_map = {
    1: '18-24',
    2: '25-34',
    3: '35-44',
    4: '45-54',
    5: '55-64',
    6: '65+'
}
df['_AGE_G'] = df['_AGE_G'].map(age_g_map)

# --- 7. _SEX ---
sex_map = {
    1: 'Hombre',
    2: 'Mujer'
}
df['_SEX'] = df['_SEX'].map(sex_map)

# --- 8. _URBSTAT ---
urbstat_map = {
    1: 'Urbano',
    2: 'Rural'
}
df['_URBSTAT'] = df['_URBSTAT'].map(urbstat_map)

# --- 9. _METSTAT ---
metstat_map = {
    1: 'Condados metropolitanos',
    2: 'Condados no metropolitanos'
}
df['_METSTAT'] = df['_METSTAT'].map(metstat_map)

# --- 10. MEDCOST1 ---
medcost_map = {
    1: 'Sí, no pudo pagar',
    2: 'No',
    7: 'No sabe / Incertidumbre',
    9: 'Se negó a responder'
}
df['MEDCOST1'] = df['MEDCOST1'].map(medcost_map)

# --- 11. CHECKUP1 ---
checkup_map = {
    1: 'En el último año (<12 meses)',
    2: 'Hace 1 a 2 años',
    3: 'Hace 2 a 5 años',
    4: 'Hace 5 o más años',
    7: 'No sabe / Incertidumbre',
    8: 'Nunca',
    9: 'Se negó a responder'
}
df['CHECKUP1'] = df['CHECKUP1'].map(checkup_map)


df.to_sql("BRFSS_2024", conn, if_exists="replace", index=False)
conn.close()

print("Tabla BRFSS_2024 actualizada correctamente con los valores mapeados.")



import sqlite3
import pandas as pd

conn = sqlite3.connect("pipeline.db")
print(pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn))
print(pd.read_sql("SELECT * FROM BRFSS_2024 LIMIT 5;", conn))
conn.close()
