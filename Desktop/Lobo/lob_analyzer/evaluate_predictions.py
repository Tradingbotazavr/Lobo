import pandas as pd
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from loguru import logger
import argparse
import glob
import os


def evaluate_predictions(file_path: str):
    """
    Loads a Parquet file with merged data, evaluates prediction accuracy,
    and prints a classification report.
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Loading data from: {file_path}")
    df = pd.read_parquet(file_path)

    # Define the columns for ground truth and predictions
    true_col = "target"
    pred_col = "pred_direction"

    # Check for required columns
    if not all(col in df.columns for col in [true_col, pred_col]):
        logger.error(f"Missing one or both required columns: '{true_col}', '{pred_col}'")
        logger.info(f"Available columns: {df.columns.tolist()}")
        return
        
    # Drop rows where prediction or target is missing
    df.dropna(subset=[true_col, pred_col], inplace=True)
    
    if df.empty:
        logger.warning("No valid rows with both target and prediction found.")
        return

    y_true = df[true_col]
    y_pred = df[pred_col]

    # Ensure labels are consistent for metrics calculation
    labels = sorted(list(set(y_true.unique()) | set(y_pred.unique())))
    if not labels:
        logger.warning("No labels found to evaluate.")
        return

    logger.info("Evaluating model predictions...")
    acc = accuracy_score(y_true, y_pred)
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    report = classification_report(y_true, y_pred, labels=labels, zero_division=0)

    logger.info(f"Accuracy: {acc:.4f}")
    logger.info(f"Confusion Matrix (Labels: {labels}):\n{cm}")
    print("\nClassification Report:\n", report)


def find_latest_file(directory: str) -> str | None:
    """Finds the most recently created file in a directory."""
    list_of_files = glob.glob(os.path.join(directory, '*.parquet'))
    if not list_of_files:
        return None
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate model predictions from a Parquet file.")
    parser.add_argument(
        "file_path",
        nargs='?',
        default=None,
        help="Path to the Parquet file to evaluate. If not provided, the latest file in 'data/merged/' will be used."
    )
    args = parser.parse_args()

    file_to_evaluate = args.file_path
    if file_to_evaluate is None:
        logger.info("No file path provided. Searching for the latest file in 'data/merged/'...")
        latest_file = find_latest_file("data/merged/")
        if latest_file:
            file_to_evaluate = latest_file
        else:
            logger.error("No Parquet files found in 'data/merged/'.")
            exit(1)
            
    evaluate_predictions(file_to_evaluate) 