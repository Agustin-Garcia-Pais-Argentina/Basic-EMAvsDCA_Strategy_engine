import pandas as pd
import os

def run_features(input_path='data/processed/data_master.parquet', output_path='data/features/market_features.parquet'):
    print("\n Iniciando cálculo de Features (Indicadores Técnicos)...")

    # Seguro 1: Verificar que el archivo maestro de entrada exista
    if not os.path.exists(input_path):
        print(f" Error: El archivo {input_path} no existe. Asegúrate de ejecutar Transform primero.")
        return None

    try:
        # 1. Carga de datos
        df = pd.read_parquet(input_path, engine='pyarrow')
        
        # 2. Cálculo de Medias Móviles Exponenciales (EMAs)
        # Usamos groupby para no mezclar los precios de diferentes empresas
        df['EMA_200D'] = df.groupby('Symbol')['Close'].transform(lambda x: x.ewm(span=200, adjust=False).mean())
        df['EMA_200W'] = df.groupby('Symbol')['Close'].transform(lambda x: x.ewm(span=1000, adjust=False).mean())

        # 5. Guardado Seguro
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True) 
        
        df.to_parquet(output_path, engine='pyarrow') 
        print(f" Features calculadas y guardadas exitosamente en: {output_path}")
        
        return output_path

    except Exception as e:
        print(f" Error procesando las features: {e}")
        return None