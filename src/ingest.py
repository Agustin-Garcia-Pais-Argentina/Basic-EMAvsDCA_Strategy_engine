from datetime import timedelta
import pandas as pd
import yfinance as yf
import os



def download_and_save_ticker_data(tickers, period, output_dir):
    
    os.makedirs(output_dir, exist_ok=True) # Creating outpu directory if it doesn't exist yet

    for ticker in tickers:
        print(f'Ingesting data for {len(ticker)} activos...')

        try:
            # data = yf.download(ticker, period=period, auto_adjust=True) #ajustamos por split y dividendos
            output_path = os.path.join(output_dir, f'{ticker}.parquet') 

            if  os.path.exists(output_path):
                df_existente = pd.read_parquet(output_path)
                print(f"Data for {ticker} already exists at {output_path}. Skipping download.")
                
                if not df_existente.empty:
                    ultima_fecha = df_existente.index.max()

                    # If the last date in the existing data is less than today, we download new data
                    start_date = (ultima_fecha + timedelta(days=1)).strftime('%Y-%m-%d')
                    print(f"Existing data for {ticker} found. Downloading new data from {start_date} to today.")

                    df_nuevo = yf.download(ticker, start=start_date, auto_adjust=True)  # Downloading from start_date to today

                    if not df_nuevo.empty:
                        # Cleaning columns if yfinance returns multi-index
                        if isinstance(df_nuevo.columns, pd.MultiIndex):
                            df_nuevo.columns = df_nuevo.columns.get_level_values(0)

                        # Joining existing data with new data
                        df_final = pd.concat([df_existente, df_nuevo])

                        # Removing duplicates if any and saving the updated data
                        df_final = df_final[~df_final.index.duplicated(keep='last')]  # Removing duplicates if any
                        df_final.to_parquet(output_path, engine='pyarrow')  # Saving the updated

                        print(f"New rows for {ticker} added successfully. Total new rows: {len(df_nuevo)-1}.")
                    else:
                        print(f"No new data found for {ticker} since {start_date}.")
                else: # No existing data, we download the complete history
                    _descarga_completa(ticker, output_path, period)
                
            else: # No existing data, we download the complete history
                _descarga_completa(ticker, output_path, period)

        except Exception as e:
            print(f"Error procesando {ticker}: {e}")
    return output_path


def _descarga_completa(ticker, output_path, period):
    print(f"Downloading complete data for {ticker} for the period: {period}.")
    df = yf.download(ticker, period=period, auto_adjust=True)  # Downloading complete history

    if not df.empty:
        # Cleaning columns if yfinance returns multi-index
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df.to_parquet(output_path, engine='pyarrow')  # Saving the complete data
        print(f"Data for {ticker} downloaded and saved successfully at {output_path}. Total rows: {len(df)}.")
    else:
        print(f"No data found for {ticker} for the period: {period}.")