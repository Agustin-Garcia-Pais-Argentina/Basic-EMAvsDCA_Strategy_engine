import pytest
import pandas as pd
import os
from src.features import run_features

@pytest.fixture
def mock_parquet_data(tmp_path):
    """
    Fixture de Pytest: Crea un archivo parquet temporal falso 
    para no ensuciar los datos reales del proyecto.
    """
    # Creamos un DataFrame con 2 activos. 
    # Usamos precios constantes (100 y 150) para que la comprobación sea exacta.
    df = pd.DataFrame({
        'Symbol': ['VOO'] * 5 + ['AAPL'] * 5,
        'Close': [100.0] * 5 + [150.0] * 5
    })
    
    input_path = tmp_path / "dummy_master.parquet"
    output_path = tmp_path / "dummy_features.parquet"
    
    df.to_parquet(input_path, engine='pyarrow')
    
    return str(input_path), str(output_path)


def test_run_features_creates_columns(mock_parquet_data):
    """Prueba 1: Verifica que el script lea el archivo y cree las columnas correctas."""
    input_path, output_path = mock_parquet_data
    
    # Ejecutamos la función
    result_path = run_features(input_path=input_path, output_path=output_path)
    
    # Verificamos que devuelva la ruta correcta y que el archivo exista
    assert result_path == output_path
    assert os.path.exists(result_path)
    
    # Verificamos que tenga exactamente las columnas que creamos
    df_result = pd.read_parquet(result_path)
    assert 'EMA_200D' in df_result.columns
    assert 'EMA_200W' in df_result.columns


def test_run_features_math_logic(mock_parquet_data):
    """Prueba 2: Verifica que el cálculo agrupado (groupby) y la matemática de la EMA funcionen."""
    input_path, output_path = mock_parquet_data
    
    run_features(input_path, output_path)
    df_result = pd.read_parquet(output_path)
    
    # Almacenamos por separado para verificar el groupby
    voo_data = df_result[df_result['Symbol'] == 'VOO']
    aapl_data = df_result[df_result['Symbol'] == 'AAPL']
    
    # LÓGICA MATEMÁTICA: Si el precio de cierre de VOO siempre fue constante, su EMA tambien debe serlo.
    assert (voo_data['EMA_200D'] == 100.0).all()
    assert (voo_data['EMA_200W'] == 100.0).all()
    
    # Lo mismo para AAPL, demostrando que el script no mezcló los precios de ambas acciones
    assert (aapl_data['EMA_200D'] == 150.0).all()
    assert (aapl_data['EMA_200W'] == 150.0).all()


def test_run_features_missing_file():
    """Prueba 3: Verifica que el código maneje correctamente cuando no encuentra el archivo."""
    ruta_falsa = "ruta/falsa/inexistente.parquet"
    
    # Tu función tiene un 'if not os.path.exists' que debe devolver None
    resultado = run_features(input_path=ruta_falsa, output_path="salida.parquet")
    
    assert resultado is None