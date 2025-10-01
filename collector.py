# collector.py
import pandas as pd
import os
from datetime import datetime
from scraper import collect_snapshot  # tu código actual lo separas en scraper.py

# OpenWeatherMap (si tienes API key la pones en secrets, si no queda en None)
OWM_KEY = os.getenv("OWM_KEY", None)

# Archivos de salida
OUT_CSV = "data/citybike_lima.csv"
OUT_EXCEL = "data/citybike_lima.xlsx"

def main():
    # Generar snapshot
    snapshot = collect_snapshot(owm_key=OWM_KEY)
    if not snapshot:
        print("No se recolectaron datos en este snapshot.")
        return

    df_new = pd.DataFrame(snapshot)

    # Cargar datos existentes si existen
    if os.path.exists(OUT_CSV):
        df_old = pd.read_csv(OUT_CSV)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    # Guardar actualizados
    os.makedirs("data", exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    df.to_excel(OUT_EXCEL, index=False)

    print(f"Se guardaron {len(df)} registros en {OUT_CSV} y {OUT_EXCEL}.")


if __name__ == "__main__":
    main()
