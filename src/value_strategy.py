import pandas as pd
import os 
import matplotlib.pyplot as plt
import numpy as np

# Seteo de parametros para impresion de pandas
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

source_path = ("./data/features/market_features.parquet")

# 1. CARGA Y SANITIZACIÓN DE FECHAS (Sin try/except para no esconder errores)
df = pd.read_parquet(source_path)
df = df.reset_index()

# Detectamos el nombre de la columna
columna_fecha = 'Date' if 'Date' in df.columns else 'index'

# Forzamos formato fecha y lo devolvemos al índice
df[columna_fecha] = pd.to_datetime(df[columna_fecha])
df.set_index(columna_fecha, inplace=True)
df.index.name = 'Date'


def run_weekly_strategy(df_master, ticker, aporte_semanal = 250.0, comision_usd = 1.0):
    
    # 1. PARAMETROS Y FILTRADO DE TICKER

    # Inicializamos variables para el seguimiento de portafolios
    cash = 0.0
    shares_ema = 0.0
    shares_dca = 0.0
    historial_ema_portafolio = []
    historial_dca_portafolio = []

    # Filtramos el DataFrame para el ticker especifico
    df_ticker = df_master[df_master['Symbol'] == ticker].copy()

    if df_ticker.empty:
        return {'Ticker': ticker, 'Error': 'No se encontraron datos para el ticker especificado.'}

    # 2. PREPARACIÓN DE DATOS

    # Compresion semanal
    df_weekly = df_ticker.resample('W').agg({
        'Open': 'first',     
        'Close': 'last',
        'EMA_200W': 'last'
    })
    df_weekly.dropna(inplace=True) # Eliminamos semanas incompletas


    # 3. SIMULACIÓN DE ESTRATEGIA SEMANAL

    # Calculo de senales y precios de compra
    df_weekly["Signal_W"] = (((df_weekly['Close'] - df_weekly['EMA_200W']) / df_weekly['EMA_200W']) <= 0.125).astype(int)
    df_weekly["Buy_Price"] = df_weekly['Open'].shift(-1)
    df_weekly.dropna(inplace=True)

    # Simulación semanal
    for index, row in df_weekly.iterrows():
        precio_compra = row['Buy_Price']
        senal = row['Signal_W']
        precio_valoracion = row['Close'] 

        # Ingreso de sueldo
        cash += aporte_semanal
        dinero_dca_efectivo = aporte_semanal - comision_usd

        if dinero_dca_efectivo > 0:
            shares_dca += dinero_dca_efectivo / precio_compra

        # Ejecución Estrategia EMA
        if senal == 1 and cash > comision_usd:
            dinero_ema_efectivo = cash - comision_usd
            shares_ema += dinero_ema_efectivo / precio_compra
            cash = 0.0

        # Valoración
        valor_dca = shares_dca * precio_valoracion
        valor_ema = (shares_ema * precio_valoracion) + cash

        historial_dca_portafolio.append(valor_dca)
        historial_ema_portafolio.append(valor_ema)


    # 4. CALCULO DE RENDIMIENTOS

    # Calculamos el dinero total físico que pusiste de tu bolsillo hasta esa semana
    df_weekly['Valor_USD_EMA'] = historial_ema_portafolio
    df_weekly['Valor_USD_DCA'] = historial_dca_portafolio
    df_weekly["Total_Invertido"] = aporte_semanal * np.arange(1, len(df_weekly) + 1)

    # Calculamos el Rendimiento Porcentual (%)
    df_weekly['Rendimiento_EMA_%'] = (df_weekly['Valor_USD_EMA'] - df_weekly['Total_Invertido']) / df_weekly['Total_Invertido'] * 100
    df_weekly['Rendimiento_DCA_%'] = (df_weekly['Valor_USD_DCA'] - df_weekly['Total_Invertido']) / df_weekly['Total_Invertido'] * 100


    # 5. Other indicators

    # Max values for drawdown calculations
    df_weekly["Peak_EMA"] = df_weekly['Valor_USD_EMA'].cummax()
    df_weekly["Peak_DCA"] = df_weekly['Valor_USD_DCA'].cummax()

    # Drawdown calculations
    drawdown_ema = (df_weekly['Valor_USD_EMA'] - df_weekly['Peak_EMA']) / df_weekly['Peak_EMA'] * 100
    drawdown_dca = (df_weekly['Valor_USD_DCA'] - df_weekly['Peak_DCA']) / df_weekly['Peak_DCA'] * 100

    # --- Sharpe Ratio ---
    # Aislamos el retorno real descontando la inyección de capital semanal
    df_weekly['Valor_USD_EMA_prev'] = df_weekly['Valor_USD_EMA'].shift(1).fillna(0)
    df_weekly['Valor_USD_DCA_prev'] = df_weekly['Valor_USD_DCA'].shift(1).fillna(0)

    base_ema = df_weekly['Valor_USD_EMA_prev'] + aporte_semanal
    base_dca = df_weekly['Valor_USD_DCA_prev'] + aporte_semanal

    df_weekly['Weekly_Return_EMA'] = (df_weekly['Valor_USD_EMA'] - base_ema) / base_ema
    df_weekly['Weekly_Return_DCA'] = (df_weekly['Valor_USD_DCA'] - base_dca) / base_dca

    # --- Tasa Libre de Riesgo ---
    # Asumimos un 4.0% anual (rendimiento conservador de Bonos del Tesoro de EE.UU.)
    risk_free_rate_annual = 0.04 
    
    # Pasamos esa tasa anual a una tasa equivalente semanal
    risk_free_rate_weekly = (1 + risk_free_rate_annual) ** (1/52) - 1 

    # Exceso de retorno (Ganancia de la estrategia MENOS lo que te ganarias sin riesgo)
    excess_return_ema = df_weekly['Weekly_Return_EMA'] - risk_free_rate_weekly
    excess_return_dca = df_weekly['Weekly_Return_DCA'] - risk_free_rate_weekly

    # Calculamos el Sharpe final anualizado
    std_ema = df_weekly['Weekly_Return_EMA'].std()
    sharpe_ema = (excess_return_ema.mean() / std_ema) * np.sqrt(52) if std_ema != 0 else 0

    std_dca = df_weekly['Weekly_Return_DCA'].std()
    sharpe_dca = (excess_return_dca.mean() / std_dca) * np.sqrt(52) if std_dca != 0 else 0


    #6. Final metrics

    roi_ema = df_weekly['Rendimiento_EMA_%'].iloc[-1]
    roi_dca = df_weekly['Rendimiento_DCA_%'].iloc[-1]
    diff = roi_ema - roi_dca
    max_dd_ema = drawdown_ema.min() # It's negative
    max_dd_dca = drawdown_dca.min() # Same here

    return {
        'Ticker': ticker,
        'ROI_EMA_%': round(roi_ema, 2),
        'ROI_DCA_%': round(roi_dca, 2),
        'Diferencia_%': round(diff, 2),
        'Max_Drawdown_EMA_%': round(max_dd_ema, 2),
        'Max_Drawdown_DCA_%': round(max_dd_dca, 2),
        'Sharpe_EMA': round(sharpe_ema, 2),
        'Sharpe_DCA': round(sharpe_dca, 2)
    }


