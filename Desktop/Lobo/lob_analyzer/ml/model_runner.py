import os
import sys

import joblib
import numpy as np
import pandas as pd
from loguru import logger

# Ensure the project root is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


class ModelRunner:
    """
    Handles loading a trained model and its artifacts for online inference.
    """

    def __init__(self, model_path: str):
        """
        Loads the model, scaler, and feature list from a joblib file.

        :param model_path: Path to the .pkl file containing training artifacts.
        """
        self.model = None
        self.scaler = None
        self.features = None
        self.logger = logger.bind(component="ModelRunner")

        try:
            artifacts = joblib.load(model_path)
            self.model = artifacts.get("model")
            self.scaler = artifacts.get("scaler")
            self.features = artifacts.get("features")

            if not all([self.model, self.scaler, self.features]):
                raise ValueError("Model artifact is missing one or more key components (model, scaler, features).")
            
            self.logger.success(f"Successfully loaded model artifacts from '{model_path}'")
            self.logger.info(f"Model expects {len(self.features)} features: {self.features}")

        except FileNotFoundError:
            self.logger.error(f"Model file not found at '{model_path}'. Inference is disabled.")
            raise
        except Exception as e:
            self.logger.exception(f"Failed to load model from '{model_path}': {e}")
            raise

    def predict(self, feature_dict: dict) -> dict:
        """
        Makes a prediction on a dictionary of features.

        :param feature_dict: A dictionary containing feature names and their values.
        :return: A dictionary with the predicted direction and confidence.
        """
        if not self.model:
            return {"error": "Model not loaded"}

        try:
            # Create the feature vector in the correct order
            feature_vector = [feature_dict.get(f) for f in self.features]
            
            # Check for missing features
            if any(v is None for v in feature_vector):
                missing = [self.features[i] for i, v in enumerate(feature_vector) if v is None]
                self.logger.warning(f"Prediction skipped. Missing features: {missing}")
                return {"error": f"Missing features: {missing}"}

            # Reshape for a single prediction
            x = np.array(feature_vector).reshape(1, -1)
            
            # Scale and predict
            x_scaled = self.scaler.transform(x)
            prediction = self.model.predict(x_scaled)[0]
            probabilities = self.model.predict_proba(x_scaled)[0]

            return {
                "direction": int(prediction),
                "confidence": float(np.max(probabilities)),
                "probabilities": {str(i-1): float(p) for i, p in enumerate(probabilities)} # {-1, 0, 1}
            }
        except Exception as e:
            self.logger.error(f"An error occurred during prediction: {e}")
            return {"error": str(e)}
