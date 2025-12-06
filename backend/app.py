"""
FastAPI application for Data Quality Anomaly Detector.
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, Any, Optional
import io
import sys
import traceback

# Add backend directory to Python path to ensure imports work regardless of execution directory
backend_dir = Path(__file__).parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from etl.extractor import DataExtractor
from etl.transformer import DataTransformer
from etl.loader import DataLoader
from anomaly_detection.detector import AnomalyDetector
from utils.data_generator import generate_mock_data

app = FastAPI(title="Data Quality Anomaly Detector API")

# CORS middleware for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],  # React dev servers
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
extractor = DataExtractor()
transformer = DataTransformer()
loader = DataLoader()
detector = AnomalyDetector()

# In-memory storage for results (in production, use a database)
results_cache: Dict[str, Any] = {}


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Data Quality Anomaly Detector API", "version": "1.0.0"}


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload and process a CSV file.
    
    Args:
        file: Uploaded CSV file
        
    Returns:
        dict: Processing results and data summary
    """
    try:
        # Read file content
        contents = await file.read()
        
        # Extract data
        df = extractor.extract_from_bytes(contents, file.filename)
        
        # Validate format (flexible - warnings instead of errors)
        validation = extractor.validate_format(df)
        
        # Only fail on critical issues, not warnings
        if not validation['valid']:
            raise HTTPException(status_code=400, detail=f"Invalid file format: {validation['issues']}")
        
        # Store column type info for later use
        column_types = validation.get('column_types', {})
        
        # Transform data (will adapt to available columns)
        df_transformed = transformer.transform(df)
        
        # Prepare for analysis
        df_prepared = loader.prepare_for_analysis(df_transformed)
        
        # Get data summary
        summary = loader.get_data_summary(df_prepared)
        
        # Cache the prepared data
        cache_key = f"data_{len(results_cache)}"
        loader.cache_data(cache_key, df_prepared)
        
        # Convert numpy types to native Python types for JSON serialization
        def convert_to_serializable(obj):
            if isinstance(obj, pd.Series):
                # Convert Series to list
                return obj.tolist()
            elif isinstance(obj, pd.DataFrame):
                # Convert DataFrame to list of dicts
                return obj.to_dict(orient='records')
            elif isinstance(obj, pd.Timestamp):
                return str(obj) if pd.notna(obj) else None
            elif isinstance(obj, (np.integer, np.floating)):
                return float(obj) if isinstance(obj, np.floating) else int(obj)
            elif isinstance(obj, dict):
                return {k: convert_to_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_serializable(item) for item in obj]
            elif isinstance(obj, set):
                return list(obj)
            elif pd.isna(obj):
                return None
            return obj
        
        # Convert summary and validation data
        summary = convert_to_serializable(summary)
        column_types = convert_to_serializable(column_types)
        warnings = convert_to_serializable(validation.get('warnings', []))
        
        # Store in results cache
        results_cache[cache_key] = {
            'data': df_prepared.to_dict(orient='records'),
            'summary': summary,
            'cache_key': cache_key,
            'column_types': column_types  # Store for detection
        }
        
        return {
            "success": True,
            "cache_key": cache_key,
            "summary": summary,
            "column_types": column_types,
            "warnings": warnings,
            "message": f"File uploaded and processed successfully. {len(df_prepared)} rows loaded."
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.post("/api/detect")
async def detect_anomalies(cache_key: Optional[str] = Query(None)):
    """
    Run anomaly detection on uploaded data.
    
    Args:
        cache_key: Key for cached data (if None, uses most recent)
        
    Returns:
        dict: Anomaly detection results
    """
    try:
        # Get cached data
        if cache_key is None:
            if not results_cache:
                raise HTTPException(status_code=400, detail="No data available. Please upload a file first.")
            # Find the most recent data cache key
            data_keys = [k for k in results_cache.keys() if k.startswith('data_')]
            if not data_keys:
                raise HTTPException(status_code=400, detail="No data available. Please upload a file first.")
            cache_key = max(data_keys, key=lambda k: int(k.split('_')[1]) if len(k.split('_')) > 1 else 0)
        
        if cache_key not in results_cache:
            raise HTTPException(status_code=404, detail="Cache key not found")
        
        cached_data = results_cache[cache_key]
        df = pd.DataFrame(cached_data['data'])
        
        # Run anomaly detection
        results = detector.detect_all(df)
        
        # Get anomaly records
        anomaly_records = detector.get_anomaly_records(df, results)
        
        # Store results
        results_cache[f"results_{cache_key}"] = {
            'results': results,
            'anomaly_records': anomaly_records.to_dict(orient='records'),
            'cache_key': cache_key
        }
        
        # Convert numpy types to native Python types for JSON serialization
        def convert_to_serializable(obj):
            if isinstance(obj, pd.Series):
                # Convert Series to list
                return obj.tolist()
            elif isinstance(obj, pd.DataFrame):
                # Convert DataFrame to list of dicts
                return obj.to_dict(orient='records')
            elif isinstance(obj, pd.Timestamp):
                return str(obj) if pd.notna(obj) else None
            elif isinstance(obj, (np.integer, np.floating)):
                return float(obj) if isinstance(obj, np.floating) else int(obj)
            elif isinstance(obj, dict):
                return {k: convert_to_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_serializable(item) for item in obj]
            elif isinstance(obj, set):
                return list(obj)
            elif pd.isna(obj):
                return None
            return obj
        
        serializable_results = convert_to_serializable(results)
        
        return {
            "success": True,
            "results": serializable_results,
            "anomaly_count": len(anomaly_records),
            "total_rows": len(df),
            "cache_key": cache_key
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"Anomaly detection error:\n{error_trace}")  # Log full traceback for debugging
        raise HTTPException(status_code=500, detail=f"Error detecting anomalies: {str(e)}")


@app.get("/api/results")
async def get_results(cache_key: Optional[str] = Query(None)):
    """
    Get anomaly detection results.
    
    Args:
        cache_key: Key for cached results (if None, uses most recent)
        
    Returns:
        dict: Cached results
    """
    try:
        if cache_key is None:
            result_keys = [k for k in results_cache.keys() if k.startswith('results_')]
            if not result_keys:
                raise HTTPException(status_code=404, detail="No results available")
            cache_key = max(result_keys, key=lambda k: int(k.split('_')[1]) if len(k.split('_')) > 1 else 0)
        elif not cache_key.startswith('results_'):
            # If provided cache_key is a data key, try to find corresponding results key
            results_key = f"results_{cache_key}"
            if results_key in results_cache:
                cache_key = results_key
            else:
                raise HTTPException(status_code=404, detail="Results not found for this cache key")
        
        if cache_key not in results_cache:
            raise HTTPException(status_code=404, detail="Results not found")
        
        cached_results = results_cache[cache_key]
        
        # Convert to serializable format
        def convert_to_serializable(obj):
            if isinstance(obj, pd.Timestamp):
                return str(obj) if pd.notna(obj) else None
            elif isinstance(obj, (np.integer, np.floating)):
                return float(obj) if isinstance(obj, np.floating) else int(obj)
            elif isinstance(obj, dict):
                return {k: convert_to_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_serializable(item) for item in obj]
            elif isinstance(obj, set):
                return list(obj)
            elif pd.isna(obj):
                return None
            return obj
        
        serializable_results = convert_to_serializable(cached_results['results'])
        
        return {
            "success": True,
            "results": serializable_results,
            "anomaly_records": cached_results['anomaly_records'],
            "cache_key": cache_key
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving results: {str(e)}")


@app.post("/api/generate-mock-data")
async def generate_mock_data_endpoint(
    num_rows: int = Query(3000, ge=1000, le=5000),
    error_rate: float = Query(0.15, ge=0.0, le=1.0)
):
    """
    Generate mock insurance payer data with intentional errors.
    
    Args:
        num_rows: Number of rows to generate (1k-5k)
        error_rate: Probability of introducing errors (0.0 to 1.0)
        
    Returns:
        dict: Generation results and file path
    """
    try:
        # Generate mock data
        data_dir = Path(__file__).parent / "data"
        try:
            data_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            error_msg = f"Failed to create data directory at {data_dir}: {str(e)}"
            raise HTTPException(status_code=500, detail=error_msg)
        
        output_path = data_dir / "mock_data.csv"
        
        # Generate mock data with detailed error handling
        try:
            df = generate_mock_data(num_rows=num_rows, error_rate=error_rate, output_path=output_path)
        except Exception as e:
            error_msg = f"Failed to generate mock data: {str(e)}"
            error_trace = traceback.format_exc()
            print(f"Mock data generation error:\n{error_trace}")  # Log for debugging
            raise HTTPException(status_code=500, detail=error_msg)
        
        # Validate generated data
        if df is None or df.empty:
            raise HTTPException(status_code=500, detail="Generated data is empty or None")
        
        # Process the generated data
        try:
            df_transformed = transformer.transform(df)
            df_prepared = loader.prepare_for_analysis(df_transformed)
        except Exception as e:
            error_msg = f"Failed to process generated data: {str(e)}"
            error_trace = traceback.format_exc()
            print(f"Data processing error:\n{error_trace}")  # Log for debugging
            raise HTTPException(status_code=500, detail=error_msg)
        
        # Cache the data
        try:
            cache_key = f"mock_data_{len(results_cache)}"
            loader.cache_data(cache_key, df_prepared)
            
            summary = loader.get_data_summary(df_prepared)
            
            # Convert numpy types to native Python types for JSON serialization
            def convert_to_serializable(obj):
                if isinstance(obj, pd.Timestamp):
                    return str(obj) if pd.notna(obj) else None
                elif isinstance(obj, (np.integer, np.floating)):
                    return float(obj) if isinstance(obj, np.floating) else int(obj)
                elif isinstance(obj, dict):
                    return {k: convert_to_serializable(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_to_serializable(item) for item in obj]
                elif isinstance(obj, set):
                    return list(obj)
                elif pd.isna(obj):
                    return None
                return obj
            
            summary = convert_to_serializable(summary)
            
            results_cache[cache_key] = {
                'data': df_prepared.to_dict(orient='records'),
                'summary': summary,
                'cache_key': cache_key
            }
        except Exception as e:
            error_msg = f"Failed to cache generated data: {str(e)}"
            error_trace = traceback.format_exc()
            print(f"Data caching error:\n{error_trace}")  # Log for debugging
            raise HTTPException(status_code=500, detail=error_msg)
        
        return {
            "success": True,
            "message": f"Generated {num_rows} rows of mock data with {error_rate*100}% error rate",
            "file_path": str(output_path),
            "cache_key": cache_key,
            "summary": summary
        }
        
    except HTTPException:
        raise
    except Exception as e:
        # Catch-all for any unexpected errors
        error_msg = f"Unexpected error generating mock data: {str(e)}"
        error_trace = traceback.format_exc()
        print(f"Unexpected error:\n{error_trace}")  # Log for debugging
        raise HTTPException(status_code=500, detail=error_msg)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

