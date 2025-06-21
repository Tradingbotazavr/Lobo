import argparse
import os
import sys

import joblib
import lightgbm as lgb
import pandas as pd
from loguru import logger
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler

# Ensure the project root is in the Python path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


def setup_logging():
    """Configures the logger for the script."""
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("logs/model_training.log", level="DEBUG", rotation="10 MB")


class ModelTrainer:
    """
    A class to handle the loading of data, training a LightGBM model,
    and saving the trained artifacts.
    """

    def __init__(self, data_path: str, model_path: str):
        self.data_path = data_path
        self.model_path = model_path
        self.df = None
        self.X = None
        self.y = None
        self.features = []
        self.model = None
        self.scaler = None

    def load_and_prepare_data(self) -> bool:
        """Loads and prepares the feature matrix (X) and target vector (y)."""
        logger.info(f"Loading data from '{self.data_path}'...")
        try:
            self.df = pd.read_parquet(self.data_path)
            if self.df.empty:
                logger.error("Loaded data is empty.")
                return False
            logger.success(f"Successfully loaded {len(self.df)} records.")
        except FileNotFoundError:
            logger.error(f"Data file not found at: {self.data_path}")
            return False
        except Exception as e:
            logger.exception(f"An error occurred while loading data: {e}")
            return False

        # Define target and drop non-feature columns
        self.y = self.df["target"]
        self.X = self.df.drop(columns=["ts", "target", "symbol"], errors='ignore')
        self.features = self.X.columns.tolist()

        logger.info(f"Features used for training ({len(self.features)}): {self.features}")
        logger.info(f"Target distribution:\n{self.y.value_counts(normalize=True)}")
        return True

    def train(self):
        """Trains the model using time-series cross-validation and a final fit."""
        logger.info("Starting model training process...")

        # Initialize scaler and model
        self.scaler = StandardScaler()
        self.model = lgb.LGBMClassifier(objective='multiclass', num_class=3, random_state=42)

        # Scale features
        self.X_scaled = self.scaler.fit_transform(self.X)

        # Time-series cross-validation
        tscv = TimeSeriesSplit(n_splits=5)
        reports = []

        logger.info("Performing time-series cross-validation...")
        for i, (train_index, test_index) in enumerate(tscv.split(self.X_scaled)):
            X_train, X_test = self.X_scaled[train_index], self.X_scaled[test_index]
            y_train, y_test = self.y.iloc[train_index], self.y.iloc[test_index]

            self.model.fit(X_train, y_train, eval_set=[(X_test, y_test)], callbacks=[lgb.early_stopping(50, verbose=False)])
            y_pred = self.model.predict(X_test)
            
            report = classification_report(y_test, y_pred, digits=3, output_dict=True)
            reports.append(report['weighted avg'])
            logger.info(f"Fold {i+1}/5 - F1-score: {report['weighted avg']['f1-score']:.3f}, Precision: {report['weighted avg']['precision']:.3f}, Recall: {report['weighted avg']['recall']:.3f}")

        avg_f1 = sum(r['f1-score'] for r in reports) / len(reports)
        logger.success(f"Cross-validation finished. Average F1-score: {avg_f1:.4f}")

        # Final model training on the full dataset
        logger.info("Training final model on the entire dataset...")
        self.model.fit(self.X_scaled, self.y)
        logger.success("Final model trained.")

        # Log feature importances
        feature_imp = pd.DataFrame(sorted(zip(self.model.feature_importances_, self.features)), columns=['Value', 'Feature'])
        logger.info(f"Top 10 Feature Importances:\n{feature_imp.sort_values(by='Value', ascending=False).head(10)}")

    def save_artifacts(self):
        """Saves the trained model, scaler, and feature list to a file."""
        if not self.model or not self.scaler:
            logger.error("Model or scaler not available. Aborting save.")
            return

        logger.info(f"Saving training artifacts to '{self.model_path}'...")
        try:
            output_dir = os.path.dirname(self.model_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            
            artifacts = {
                "model": self.model,
                "scaler": self.scaler,
                "features": self.features
            }
            joblib.dump(artifacts, self.model_path)
            logger.success("Training artifacts saved successfully.")
        except Exception as e:
            logger.exception(f"Failed to save artifacts: {e}")

    def run(self):
        """Executes the full training pipeline."""
        if self.load_and_prepare_data():
            self.train()
            self.save_artifacts()


def main():
    """Main execution function with command-line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Train a LightGBM model on time-series feature data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-i", "--input-file",
        type=str,
        default="data/ml_dataset.parquet",
        help="Path to the labeled Parquet dataset."
    )
    parser.add_argument(
        "-o", "--output-file",
        type=str,
        default="models/lgbm_model_v1.pkl",
        help="Path to save the trained model artifacts."
    )
    args = parser.parse_args()

    setup_logging()
    trainer = ModelTrainer(data_path=args.input_file, model_path=args.output_file)
    trainer.run()


if __name__ == "__main__":
    main()
