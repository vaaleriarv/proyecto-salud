# Revisión y Corrección del Código de Ingesta y Limpieza

##  |||  YO HARE EL CAMBIO DE ODEPA Y FOODDATACENTRAL !!!                        |||
##  |||  PERO DEJARE SIN TOCAR EL DE NHANES Y BRFSS PARA NO CAMBIAR TU LIMPIEZA  |||
##  |||                                                                          |||

### Problemas Identificados en la Ingesta (1_ingesta.py)

**Problemas Críticos:**

1. **ODEPA**: La descarga usa API con límite de 50,000 registros pero el dataset tiene 268,894 registros. Se perderán datos.
2. **FoodData Central**: Extrae todos los CSV sin filtrar, generando muchas tablas innecesarias.
3. **NHANES**: Crea tabla combinada "NHANES_2021" con merge outer que genera matriz muy grande y con muchos NaN.

### Problemas en Limpieza NHANES (LimpiezaNHANES.py)

1. Lee tabla "NHANES_2021" que contiene todas las columnas combinadas (redundante).
2. Vuelve a hacer el mismo merge que ya se hizo en ingesta.
3. Sobrescribe la tabla original sin necesidad.
4. Mapeos de DEMO_L aplicados a columnas que tienen prefijo "DEMO_L_".

### Problemas en Limpieza BRFSS (LimpiezaBRFSS.py)
2. Algunos valores numéricos podrían no convertirse correctamente.
