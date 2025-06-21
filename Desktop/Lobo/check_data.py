import pandas as pd
import glob
import os
import sys

def check_latest_datafile():
    """
    Finds the most recent Parquet file, loads it, and prints its structure
    and a sample of the data.
    """
    try:
        # Find the latest file in the data/merged directory
        list_of_files = glob.glob('data/merged/*.parquet')
        if not list_of_files:
            print("Ошибка: Не найдены Parquet файлы в директории 'data/merged/'.")
            print("Пожалуйста, сначала запустите сбор данных (_temp_collect_data.bat).")
            sys.exit(1)
            
        latest_file = max(list_of_files, key=os.path.getctime)
        print(f"Анализируем самый свежий файл: {latest_file}\n")

        # Load the data using pandas
        df = pd.read_parquet(latest_file)

        # --- Verification ---
        print("="*50)
        print("1. Проверка колонок:")
        print("="*50)
        print("Всего колонок:", len(df.columns))
        print(list(df.columns))
        print("\n")

        # Check for new columns
        new_features = [
            'bid_volume_near', 'ask_volume_near', 'avg_price', 
            'buy_vol_speed', 'sell_vol_speed'
        ]
        missing_features = [f for f in new_features if f not in df.columns]
        if not missing_features:
            print("Отлично! Все новые признаки присутствуют в данных.")
        else:
            print(f"ВНИМАНИЕ! Отсутствуют следующие признаки: {missing_features}")
        
        print("\n" + "="*50)
        print("2. Пример данных (первые 5 строк):")
        print("="*50)
        print(df.head().to_markdown(index=False))
        print("\n")
        
        print("="*50)
        print("3. Проверка значений (некоторые новые колонки):")
        print("="*50)
        # Display a subset of columns for clarity
        check_cols = ['ts', 'price', 'avg_price', 'buy_vol_speed', 'sell_vol_speed', 'bid_volume_near', 'ask_volume_near']
        cols_to_display = [c for c in check_cols if c in df.columns]
        
        if cols_to_display:
            print(df[cols_to_display].head().to_markdown(index=False))
        else:
            print("Не найдены колонки для проверки значений.")


    except Exception as e:
        print(f"Произошла ошибка при анализе файла: {e}")

if __name__ == "__main__":
    check_latest_datafile() 