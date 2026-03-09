import pandas as pd
import pytest
from src.features import calcular_ema  # ajustá al nombre real

def test_ema_no_retorna_nulls_en_periodo_valido():
    """La EMA no debería tener nulls después del período de calentamiento."""
    precios = pd.Series([100.0] * 300)  # 300 semanas de precio constante
    resultado = calcular_ema(precios, periodo=200)
    assert resultado.iloc[200:].isna().sum() == 0

def test_ema_precio_constante_igual_al_precio():
    """Con precio constante, EMA debe converger al mismo precio."""
    precios = pd.Series([150.0] * 300)
    resultado = calcular_ema(precios, periodo=200)
    assert abs(resultado.iloc[-1] - 150.0) < 0.01