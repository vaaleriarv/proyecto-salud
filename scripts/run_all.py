import subprocess
import os

# Lista de scripts en orden de ejecución
scripts = [
    "1_ingesta.py",
    "2_LimpiezayTransformación.py/LyT_BRFSS.py",
    "2_LimpiezayTransformación.py/LyT_NHANES.py",
    "2_LimpiezayTransformación.py/LyT_ODEPA.py",
    "2_LimpiezayTransformación.py/LyT_OFDC.py",
    "4_integracion.py"
]

for s in scripts:
    script_path = os.path.join("scripts", s)
    print(f"\n=== Ejecutando {script_path} ===\n")
    try:
        subprocess.run(["python", script_path], check=True)
        print(f"{s} ejecutado correctamente \n")
    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar {s}: {e} \n")
    except Exception as e:
        print(f"Excepción inesperada en {s}: {e} \n")

print("Ejecución de todos los scripts finalizada")
