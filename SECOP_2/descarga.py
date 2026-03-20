import pandas as pd
import requests
import time

url = 'https://www.datos.gov.co/resource/p6dx-8zbt.json'

limit = 1000
offset = 0
data = []

while True:
    params = {
        '$limit': limit,
        '$offset': offset,
        '$where': "fecha_de_publicacion >= '2026-02-16' AND fecha_de_publicacion < '2026-03-16'"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        print("Error:", response.status_code)
        print(response.text)  # 🔥 esto te dice EXACTAMENTE qué falló
        break

    batch = response.json()

    if not batch:
        break

    data.extend(batch)

    print(f"Descargadas {len(data)} filas...")

    offset += limit
    time.sleep(0.5)

df = pd.DataFrame(data)

df.to_parquet('secop.parquet', index=False, engine='pyarrow')

print("Archivo guardado correctamente ✅")