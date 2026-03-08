import pyarrow 
import pandas as pd
from pathlib import Path

def run_transform(input_folder='data/raw/', output_folder='data/processed/'):
    print("\n Iniciando fase de Transformación (Merge de Tickers)...")
    
    data_dir = Path(input_folder)
    dfs = [] # creamos una lista vacía para almacenar los dataframes

    # Seguro 1: Verificar que la carpeta exista
    if not data_dir.exists():
        print(f" Error: El directorio {input_folder} no existe. ¿Corriste ingest.py?")
        return None

    # Iteramos sobre cada archivo parquet del directorio
    archivos_procesados = 0
    for parquet_file in data_dir.glob('*.parquet'): 
        try:
            df = pd.read_parquet(parquet_file, engine='pyarrow') 
            
            # Limpieza de columnas (aplanar multiindex si existe)
            df.columns = df.columns.get_level_values(0) 
            
            # Agregamos la columna del ticker
            df['Symbol'] = parquet_file.stem 
            dfs.append(df) 
            archivos_procesados += 1
            
        except Exception as e:
            print(f" Error loading data from {parquet_file}: {e}")

    # Seguro 2: Verificar que se hayan encontrado archivos
    if not dfs:
        print(" Error: No se encontraron archivos .parquet para procesar.")
        return None

    # Concatenamos todos los dataframes en uno solo
    df_final = pd.concat(dfs) 
    print(f" Data set Maestro creado combinando {archivos_procesados} tickers.")
    print(f" Total de filas en el DataFrame final: {len(df_final)}") 

    # Guardado
    output_dir = Path(output_folder)
    output_dir.mkdir(parents=True, exist_ok=True) 
    output_file = output_dir / 'data_master.parquet' 
    
    df_final.to_parquet(output_file, engine='pyarrow') 
    print(f" Archivo Maestro guardado en: {output_file}")
    
    # ¡El paso crucial! Devolvemos la ruta en formato string para que el main.py la use
    return str(output_file)