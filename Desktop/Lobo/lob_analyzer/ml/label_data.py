import argparse
import glob
import os
import sys

import numpy as np
import pandas as pd
from loguru import logger

# Ensure the project root is in the Python path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


def setup_logging():
    """Configures the logger for the script."""
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("logs/label_generator.log", level="DEBUG", rotation="10 MB")


def load_data_from_directory(input_dir: str) -> pd.DataFrame:
    """
    Loads all Parquet files from a directory into a single, sorted DataFrame.

    :param input_dir: Path to the directory containing Parquet files.
    :return: A concatenated and sorted DataFrame.
    """
    logger.info(f"Searching for Parquet files in '{input_dir}'...")
    parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))

    if not parquet_files:
        logger.error(f"No .parquet files found in the directory: {input_dir}")
        return pd.DataFrame()

    logger.info(f"Found {len(parquet_files)} files to merge.")
    df_list = [pd.read_parquet(file) for file in parquet_files]
    df = pd.concat(df_list, ignore_index=True)

    # Ensure data is sorted by timestamp for correct labeling
    df.sort_values(by="ts", inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.success(f"Loaded and merged a total of {len(df)} records.")
    return df


def generate_labels(df: pd.DataFrame, window_seconds: int, threshold_pct: float) -> pd.DataFrame:
    """
    Generates target labels for price movement using a vectorized approach.

    :param df: The input DataFrame, sorted by timestamp.
    :param window_seconds: The time in seconds to look into the future for price changes.
    :param threshold_pct: The percentage change required to be labeled as 1 or -1.
    :return: DataFrame with an added 'target' column.
    """
    if df.empty:
        logger.warning("Input DataFrame is empty. Skipping label generation.")
        return df

    logger.info(f"Generating labels with a {window_seconds}s window and {threshold_pct:.4f} threshold...")

    # For merge_asof, timestamps need to be in datetime format
    df["ts_dt"] = pd.to_datetime(df["ts"], unit="s")

    # Find the future price by shifting the price series
    df_future = df[["ts_dt", "price"]].copy()
    df_future.rename(columns={"price": "future_price"}, inplace=True)
    
    # Use merge_asof for a high-performance, vectorized lookup
    merged_df = pd.merge_asof(
        left=df,
        right=df_future,
        on="ts_dt",
        direction="forward",
        tolerance=pd.Timedelta(seconds=window_seconds)
    )

    # Calculate the percentage change to the future price
    merged_df["pct_change"] = (merged_df["future_price"] - merged_df["price"]) / merged_df["price"]

    # Define conditions for the target labels
    conditions = [
        merged_df["pct_change"] > threshold_pct,
        merged_df["pct_change"] < -threshold_pct,
    ]
    choices = [1, -1]  # 1 for up, -1 for down

    # Create the target column, defaulting to 0 (neutral)
    merged_df["target"] = np.select(conditions, choices, default=0)

    # Clean up rows where a future price could not be found (at the end of the dataset)
    initial_rows = len(merged_df)
    merged_df.dropna(subset=["future_price"], inplace=True)
    final_rows = len(merged_df)
    logger.info(f"Removed {initial_rows - final_rows} rows from the end due to lack of future data.")

    # Drop temporary columns and convert target to integer
    merged_df.drop(columns=["ts_dt", "future_price", "pct_change"], inplace=True)
    merged_df["target"] = merged_df["target"].astype(int)

    logger.success(f"Label generation complete. Final dataset has {final_rows} records.")
    logger.info(f"Target distribution:\n{merged_df['target'].value_counts(normalize=True)}")
    
    return merged_df


def main():
    """Main execution function with command-line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Generate ML labels for time-series feature data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-i", "--input-dir",
        type=str,
        default="data/merged/",
        help="Input directory containing merged .parquet files."
    )
    parser.add_argument(
        "-o", "--output-file",
        type=str,
        default="data/ml_dataset.parquet",
        help="Path to save the final labeled .parquet dataset."
    )
    parser.add_argument(
        "-w", "--window",
        type=int,
        default=5,
        help="Future time window in seconds for calculating the target."
    )
    parser.add_argument(
        "-t", "--threshold",
        type=float,
        default=0.001,
        help="Minimum percentage change to be considered a non-neutral move."
    )
    args = parser.parse_args()

    setup_logging()

    # 1. Load data
    df = load_data_from_directory(args.input_dir)

    # 2. Generate labels
    labeled_df = generate_labels(df, args.window, args.threshold)

    # 3. Save final dataset
    if not labeled_df.empty:
        try:
            output_dir = os.path.dirname(args.output_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            labeled_df.to_parquet(args.output_file, index=False)
            logger.success(f"Successfully saved labeled dataset to '{args.output_file}'")
        except Exception as e:
            logger.exception(f"Failed to save the final dataset: {e}")


if __name__ == "__main__":
    main()