"""
FastAPI application for Data Quality Anomaly Detector.
"""

from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
import io
import json

from etl.extractor import DataExtractor
from etl.transformer import DataTransformer
from etl.loader import DataLoader
from anomaly_detection.detector import AnomalyDetector
from utils.data_generator import generate_mock_data

# Initialize FastAPI app
app = FastAPI(title="Data Quality Anomaly Detector API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all origins for now
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
extractor = DataExtractor()
transformer = DataTransformer()
loader = DataLoader()
detector = AnomalyDetector()

# In-memory cache for data and results
data_cache = {}
results_cache = {}


def convert_to_native_types(obj: Any) -> Any:
    """
    Convert numpy/pandas types to native Python types for JSON serialization.
    """
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient='records')
    elif isinstance(obj, dict):
        return {key: convert_to_native_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_native_types(item) for item in obj]
    elif pd.isna(obj):
        return None
    else:
        return obj


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Data Quality Anomaly Detector API", "version": "1.0.0"}


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload and process a CSV file.
    
    Returns summary and column type information.
    """
    try:
        # Read file content
        contents = await file.read()
        
        # Extract data
        df = extractor.extract_from_bytes(contents, file.filename)
        
        # Transform data
        df_transformed = transformer.transform(df)
        
        # Prepare for analysis
        df_prepared = loader.prepare_for_analysis(df_transformed)
        
        # Validate format
        validation = extractor.validate_format(df_prepared)
        
        # Get data summary
        summary = loader.get_data_summary(df_prepared)
        
        # Cache the data
        cache_key = f"data_{len(data_cache)}"
        loader.cache_data(cache_key, df_prepared)
        data_cache[cache_key] = df_prepared
        
        # Convert to native types
        response_data = {
            "success": True,
            "cache_key": cache_key,
            "summary": convert_to_native_types(summary),
            "column_types": convert_to_native_types(validation.get('column_types', {})),
            "warnings": validation.get('warnings', []),
            "message": f"File uploaded and processed successfully. {len(df_prepared)} rows loaded."
        }
        
        return JSONResponse(content=response_data)
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.post("/api/detect")
async def detect_anomalies(cache_key: Optional[str] = Query(None, alias="cache_key")):
    """
    Run anomaly detection on uploaded data.
    
    Query Parameters:
        cache_key: Optional key for cached data (uses most recent if not provided)
    """
    try:
        # Get data from cache
        if cache_key and cache_key in data_cache:
            df = data_cache[cache_key]
        elif len(data_cache) > 0:
            # Use most recent cache entry
            cache_key = max(data_cache.keys(), key=lambda k: int(k.split('_')[1]) if k.startswith('data_') else 0)
            df = data_cache[cache_key]
        else:
            raise HTTPException(status_code=400, detail="No data available. Please upload a file first.")
        
        # Run anomaly detection
        results = detector.detect_all(df)
        
        # Cache results
        results_cache_key = f"results_{cache_key}"
        results_cache[results_cache_key] = results
        
        # Get anomaly records
        anomaly_records = detector.get_anomaly_records(df, results)
        
        # Convert to native types
        response_data = {
            "success": True,
            "cache_key": cache_key,
            "results": convert_to_native_types(results),
            "anomaly_count": len(results['summary']['anomaly_indices']),
            "total_rows": len(df),
            "anomaly_records": convert_to_native_types(anomaly_records)
        }
        
        return JSONResponse(content=response_data)
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error detecting anomalies: {str(e)}")


@app.get("/api/results")
async def get_results(cache_key: Optional[str] = Query(None, alias="cache_key")):
    """
    Get cached anomaly detection results.
    
    Query Parameters:
        cache_key: Optional key for cached results
    """
    try:
        # Find results cache key
        if cache_key:
            results_key = f"results_data_{cache_key.split('_')[1]}" if cache_key.startswith('data_') else f"results_{cache_key}"
        else:
            # Use most recent results
            if not results_cache:
                raise HTTPException(status_code=404, detail="No results available. Please run detection first.")
            results_key = max(results_cache.keys())
        
        if results_key not in results_cache:
            raise HTTPException(status_code=404, detail="Results not found for the provided cache key.")
        
        results = results_cache[results_key]
        
        # Get corresponding data
        data_key = results_key.replace('results_', '')
        if data_key in data_cache:
            df = data_cache[data_key]
            anomaly_records = detector.get_anomaly_records(df, results)
        else:
            anomaly_records = pd.DataFrame()
        
        # Convert to native types
        response_data = {
            "success": True,
            "cache_key": data_key,
            "results": convert_to_native_types(results),
            "anomaly_count": len(results['summary']['anomaly_indices']),
            "total_rows": len(df) if data_key in data_cache else 0,
            "anomaly_records": convert_to_native_types(anomaly_records)
        }
        
        return JSONResponse(content=response_data)
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving results: {str(e)}")


@app.post("/api/generate-mock-data")
async def generate_mock_data_endpoint(
    num_rows: int = Query(3000, ge=1000, le=5000, alias="num_rows"),
    error_rate: float = Query(0.15, ge=0.0, le=1.0, alias="error_rate")
):
    """
    Generate mock insurance payer data with intentional errors.
    
    Query Parameters:
        num_rows: Number of rows to generate (1000-5000, default: 3000)
        error_rate: Probability of introducing errors (0.0-1.0, default: 0.15)
    """
    try:
        # Generate mock data
        df = generate_mock_data(num_rows=num_rows, error_rate=error_rate)
        
        # Transform and prepare data
        df_transformed = transformer.transform(df)
        df_prepared = loader.prepare_for_analysis(df_transformed)
        
        # Validate format
        validation = extractor.validate_format(df_prepared)
        
        # Get data summary
        summary = loader.get_data_summary(df_prepared)
        
        # Cache the data
        cache_key = f"data_{len(data_cache)}"
        loader.cache_data(cache_key, df_prepared)
        data_cache[cache_key] = df_prepared
        
        # Convert DataFrame to CSV string for download
        csv_buffer = io.StringIO()
        df_prepared.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        # Convert to native types
        response_data = {
            "success": True,
            "cache_key": cache_key,
            "summary": convert_to_native_types(summary),
            "column_types": convert_to_native_types(validation.get('column_types', {})),
            "warnings": validation.get('warnings', []),
            "message": f"Mock data generated successfully. {len(df_prepared)} rows created.",
            "csv_content": csv_content
        }
        
        return JSONResponse(content=response_data)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating mock data: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

