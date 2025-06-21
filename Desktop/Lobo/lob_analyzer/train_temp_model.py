import pandas as pd
import joblib
import glob
import os
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from loguru import logger


def find_latest_file(directory: str) -> str | None:
    """Finds the most recently created file in a directory."""
    list_of_files = glob.glob(os.path.join(directory, '*.parquet'))
    if not list_of_files:
        return None
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file


def train_and_save_temp_model(data_path: str, output_path: str):
    """
    Trains a simple model on the collected data and saves it in the format
    expected by ModelRunner.
    """
    if not os.path.exists(data_path):
        logger.error(f"Data file not found: {data_path}")
        return

    logger.info(f"Loading data from {data_path} to train temporary model...")
    df = pd.read_parquet(data_path)

    # --- Feature Engineering & Selection ---
    features = [
        "mid_price", "imbalance", "bid_volume", "ask_volume", "activity_spike"
    ]
    target = "target"

    required_cols = features + [target]
    if not all(col in df.columns for col in required_cols):
        logger.error(f"Missing required columns. Needed: {required_cols}, Got: {df.columns.tolist()}")
        return

    df = df.dropna(subset=required_cols)
    if df.empty:
        logger.error("No valid data left after dropping NaNs. Cannot train model.")
        return
        
    X = df[features]
    y = df[target]

    logger.info(f"Training on {len(X)} samples.")

    # --- Model Training ---
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    model = LogisticRegression(multi_class="multinomial", class_weight='balanced', max_iter=1000)
    model.fit(X_train_scaled, y_train)

    accuracy = model.score(X_test_scaled, y_test)
    logger.info(f"Test Accuracy of temporary model: {accuracy:.4f}")

    # --- Save Artifacts ---
    artifacts = {
        "model": model,
        "scaler": scaler,
        "feature_names": features
    }
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    joblib.dump(artifacts, output_path)
    logger.success(f"Model artifacts saved to: {output_path}")


if __name__ == "__main__":
    output_model_path = "models/best_model.pkl"
    
    data_dir = "data/merged/"
    latest_data_file = find_latest_file(data_dir)

    if latest_data_file:
        train_and_save_temp_model(latest_data_file, output_model_path)
    else:
        logger.error(f"No data files found in {data_dir}. Run data collection first.") 