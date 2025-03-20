from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import pandas as pd
from autogluon.timeseries import TimeSeriesPredictor
import logging
import os

# Configure logging to track important information during prediction
logging.basicConfig(
    filename="prediction_logs.log",  # Log file name
    level=logging.INFO,  # Log level (INFO to record important information)
    format="%(asctime)s - %(levelname)s - %(message)s"  # Log format: includes timestamp, log level, and message
)

# Create a FastAPI application instance
app = FastAPI()

# Load the AutoGluon TimeSeries model
model_path = os.path.join("TrainedModel") # Replace with your actual model path
try:
    predictor = TimeSeriesPredictor.load(model_path)  # Load the trained TimeSeries model
    model_name = 'AutoETS'  # Specify the model name used for prediction
    logging.info("Model successfully loaded from path: %s", model_path)  # Log successful model loading
except Exception as e:
    logging.error("Failed to load model from path: %s. Error: %s", model_path, str(e))  # Log failure
    raise  # Stop the application if the model fails to load

# Define the input data structure for API requests
class TimeSeriesRecord(BaseModel):
    timestamp: str  # Time series timestamp in string format
    Value: float    # Value of the time series at the given timestamp
    item_id: str    # Identifier for the specific time series

percentiles = ['mean', '0.025', '0.975']

@app.post("/predict")
def predict(data: List[TimeSeriesRecord]):
    """
    Receives time series data, performs predictions using AutoGluon, and returns the selected results.
    """
    # Convert input JSON data to Pandas DataFrame
    df = pd.DataFrame([record.dict() for record in data])

    df['timestamp'] = pd.to_datetime(df['timestamp'])  # Ensure timestamp is in datetime format

    # Perform prediction
    try:
        predictions = predictor.predict(df, model=model_name)  # Perform prediction
        print("Prediction successful. Results:")
        print(predictions)  # Print full prediction results

        # Select only mean, 0.025, and 0.975 columns from the results
        selected_results = predictions[percentiles].reset_index()

        selected_results.rename(columns={'mean': 'prediction', '0.025': 'lowerBound', '0.975': 'upperBound'}, inplace=True)
        # Return the selected columns as a JSON response
        return selected_results.to_dict(orient="records")
    except Exception as e:
        print(f"Prediction failed. Error: {str(e)}")
        return {"error": "Prediction failed due to an internal error."}