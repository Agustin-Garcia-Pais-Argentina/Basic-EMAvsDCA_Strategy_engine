from src.ingest import download_and_save_ticker_data as ingest
from src.transform import run_transform as transform
from src.features import run_features as features
from src.value_strategy import run_weekly_strategy as value_strategy
import pandas as pd

if __name__ == "__main__":
    
    # --- CONFIGURACIÓN ---
    tickers = ['VOO', 'EWZ', 'EEM', 'VXUS', 'VTI', 'QQQ']
    aporte_usd = 250.0
    
    print(" INICIANDO PIPELINE DE BACKTESTING CUANTITATIVO ")
    
    # 1. Ingesta (Extract)
    ruta_raw = ingest(tickers=tickers, period="10y", output_dir='data/raw/')
    
    # 2. Transformación (Load)
    ruta_master = transform(input_folder='data/raw/', output_folder='data/processed/')
    
    # 3. Features (Transform)
    if ruta_master:
        ruta_features = features(input_path=ruta_master, output_path='data/features/market_features.parquet')
    
    # 4. Simulación de Estrategias
    if ruta_features:
        print("\n Iniciando simulaciones por activo...")
        df_final = pd.read_parquet(ruta_features)
        resultados = []
        
        for ticker in tickers:
            resultado = value_strategy(df_final, ticker, aporte_usd)
            if 'Error' not in resultado:
                resultados.append(resultado)
                
        # 5. Reporte Final
        df_resultados = pd.DataFrame(resultados)
        print("\n --- RANKING FINAL DE ESTRATEGIAS (10 AÑOS) --- ")
        print(df_resultados.sort_values(by='Diferencia_%', ascending=False).to_string(index=False))