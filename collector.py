# collector.py
import os
import pandas as pd
from prueba_5 import collect_snapshot

OWM_KEY = os.getenv("OWM_KEY", None)

OUT_CSV = "data/citybike_lima.csv"
OUT_XLSX = "data/citybike_lima.xlsx"

def main():
    snapshot = collect_snapshot(owm_key=OWM_KEY)
    if not snapshot:
        print("⚠️ No se recolectaron datos en este snapshot.")
        return

    df_new = pd.DataFrame(snapshot)

    # Si ya existe el archivo, concatenar con los datos anteriores
    if os.path.exists(OUT_CSV):
        df_old = pd.read_csv(OUT_CSV)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    os.makedirs("data", exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    df.to_excel(OUT_XLSX, index=False)
    print(f"✅ Guardados {len(df_new)} nuevas filas (total acumulado: {len(df)})")

if __name__ == "__main__":
    main()
