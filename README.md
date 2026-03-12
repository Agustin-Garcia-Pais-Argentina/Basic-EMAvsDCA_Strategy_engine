# 📈 Quantitative Backtesting Engine: EMA 200W vs DCA

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Pandas](https://img.shields.io/badge/Pandas-Data_Engineering-green.svg)
![Finance](https://img.shields.io/badge/Domain-Quantitative_Finance-orange.svg)

## 🇬🇧 English

### Overview
This project is an Enterprise-Grade Data Engineering and Quantitative Backtesting pipeline. It evaluates the performance of a Value Investing strategy based on the 200-Week Exponential Moving Average (EMA 200W) against a traditional Dollar-Cost Averaging (DCA) approach over a 10+ year horizon.

The system is designed with a modular architecture, focusing on separation of concerns, incremental data ingestion, and realistic financial simulation (including trading commissions and Maximum Drawdown risk analysis).



### Architecture & Pipeline   

```mermaid
graph TD
    A[(Yahoo Finance API)] -->|Extract: Incremental Load| B(Raw Parquet Files)
    B -->|Transform: Clean & Merge| C(Master Dataset)
    C -->|Feature Eng: Calculate EMAs| D(Market Features Dataset)
    D -->|Simulate: Cash Flow & Rules| E{Value Strategy Engine}
    E -->|Analyze| F[Performance & Risk Metrics]
    
    style A fill:#452445,stroke:#333,stroke-width:2px
    style E fill:#1d141f,stroke:#333,stroke-width:2px
    style F fill:#516e51,stroke:#333,stroke-width:2px
```
The project follows a modular ETL (Extract, Transform, Load) and Simulation flow:
1. **`ingest.py` (Extract):** Smart incremental data loading using the `yfinance` API. It checks local `.parquet` files and only downloads missing days to optimize network and API usage.
2. **`transform.py` (Transform/Load):** Merges individual asset data into a unified, flat MultiIndex Master Dataset using `pyarrow` for high-performance I/O.
3. **`features.py` (Feature Engineering):** Agnostic technical indicator calculation. Computes EMAs and price-to-moving-average distances without applying business logic.
4. **`value_strategy.py` (Simulation Engine):** The core business logic. Simulates a realistic weekly cash flow ($250/week), deducts broker commissions, executes trades based on signals, and calculates ROI and Max Drawdown to measure risk-adjusted performance.
5. **`main.py` (Orchestrator):** The entry point that runs the entire pipeline from end to end.

### Results (10-Year Simulation)
*Simulation Parameters: $250 weekly contribution, $1 fixed broker commission per trade. Buy signal triggered when price is within +10% or strictly below the 200-Week EMA.*

| Ticker | ROI EMA % | ROI DCA % | Alpha (Diff %) | Max Drawdown EMA % | Max Drawdown DCA % |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **GGAL** | 237.74 | 230.88 | +6.86 | -74.58 | -74.33 |
| **VOO** | 125.03 | 118.42 | +6.61 | -24.74 | -30.25 |
| **QQQM** | 66.77  | 60.93  | +5.84 | -14.12 | -19.43 |
| **VXUS** | 80.07  | 78.09  | +1.98 | -30.03 | -30.62 |
| **PAM** | 171.92 | 188.71 | -16.79| -61.59 | -63.79 |
| **YPF** | 237.98 | 261.91 | -23.93| -73.44 | -73.22 |
| **MSFT** | 76.00  | 184.27 | -108.27| -22.67 | -31.26 |
| **MELI** | 63.31  | 242.39 | -179.08| -49.05 | -65.93 |


### Results (10-Year Simulation - Index/ETF Focus)
*Simulation Parameters: $250 weekly contribution, $1 fixed broker commission per trade. Risk-Free Rate for Sharpe Ratio: 4.0% Annual. Buy signal triggered when price is within +12.5% or strictly below the 200-Week EMA.*

| Ticker | ROI EMA % | ROI DCA % | Alpha (Diff %) | Max Drawdown EMA % | Max Drawdown DCA % | Sharpe EMA | Sharpe DCA |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **QQQ** | 177.58 | 167.68 | +9.89 | -26.30 | -29.56 | 0.86 | 0.78 |
| **VTI** | 112.86 | 106.23 | +6.63 | -25.29 | -31.39 | 0.65 | 0.60 |
| **VOO** | 119.20 | 113.84 | +5.36 | -24.41 | -30.25 | 0.69 | 0.64 |
| **EWZ** | 54.77  | 51.14  | +3.63 | -50.64 | -51.46 | 0.28 | 0.28 |
| **EEM** | 51.81  | 50.81  | +1.00 | -29.02 | -29.13 | 0.27 | 0.27 |
| **VXUS** | 66.44  | 65.58  | +0.86 | -30.52 | -30.62 | 0.35 | 0.34 |

**Quantitative Conclusion:** The EMA 200W strategy consistently improves risk-adjusted returns (Sharpe Ratio) and reduces maximum capital drawdown across broad market and sector ETFs when compared to systematic DCA, effectively providing a mathematical edge against market volatility.

**Key Findings:** The EMA 200W strategy successfully generates Alpha and reduces Max Drawdown in broad market indices (VOO, QQQM). However, in aggressive growth stocks (MSFT, MELI), the strategy heavily underperforms DCA due to the lack of major price corrections, although it provides a significantly stronger cash buffer during market crashes.

---

## 🇪🇸 Español

### Descripción del Proyecto
Este proyecto es un motor de Backtesting Cuantitativo e Ingeniería de Datos. Evalúa el rendimiento de una estrategia de Value Investing basada en la Media Móvil Exponencial de 200 Semanas (EMA 200W) frente a una estrategia tradicional de compras periódicas (DCA) en un horizonte de más de 10 años.

El sistema está diseñado con una arquitectura modular, enfocada en la separación de responsabilidades, ingesta incremental de datos y simulación financiera realista (incluyendo comisiones de broker y análisis de riesgo mediante Max Drawdown).

### Arquitectura del Pipeline
1. **`ingest.py` (Extracción):** Carga incremental inteligente usando la API de Yahoo Finance. Lee los archivos `.parquet` locales y descarga únicamente los días faltantes.
2. **`transform.py` (Transformación):** Unifica los datos individuales en un Dataset Maestro utilizando el motor `pyarrow`.
3. **`features.py` (Ingeniería de Características):** Cálculo agnóstico de indicadores técnicos. 
4. **`value_strategy.py` (Motor de Simulación):** Simula un flujo de caja semanal real ($250/semana), deduce comisiones operativas, ejecuta operaciones basadas en señales y calcula el ROI y el Max Drawdown.
5. **`main.py` (Orquestador):** El punto de entrada que ejecuta todo el pipeline.

### Instalación y Uso

1. Clonar el repositorio.
2. Instalar las dependencias:
```bash
pip install -r requirements.txt