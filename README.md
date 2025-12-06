# Data Quality Anomaly Detector

A comprehensive data quality monitoring system that automatically detects duplicates, missing values, inconsistencies, and outliers using statistical methods and machine learning. The system adapts to any CSV file structure by auto-detecting column types and running appropriate anomaly detection methods.

## Features

- **Flexible ETL Pipeline**: Extract, transform, and load data from CSV files with any column structure
- **Auto-Detection**: Automatically detects column types (numeric, date, text, ID) and adapts detection methods
- **Anomaly Detection**:
  - Statistical methods: Z-score and IQR (Interquartile Range)
  - Machine Learning: Isolation Forest and Local Outlier Factor (LOF)
  - Data quality checks: Duplicates, missing values, format inconsistencies
- **Web Dashboard**: Real-time visualization of anomalies with interactive charts
- **Mock Data Generator**: Generate synthetic data with intentional errors for testing (different results each time)

## Project Structure

```
data_quality_anomaly_detector/
├── backend/
│   ├── app.py                 # FastAPI main application
│   ├── etl/
│   │   ├── extractor.py      # Data extraction module
│   │   ├── transformer.py    # Data transformation module
│   │   └── loader.py         # Data loading module
│   ├── anomaly_detection/
│   │   ├── statistical.py    # Z-score, IQR methods
│   │   ├── ml_models.py      # Isolation Forest, LOF
│   │   └── detector.py       # Main anomaly detection orchestrator
│   ├── data/
│   │   └── mock_data.csv     # Generated mock insurance data
│   └── utils/
│       └── data_generator.py # Mock data generator with intentional errors
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── Dashboard.jsx
│   │   │   ├── DataUpload.jsx
│   │   │   ├── AnomalyTable.jsx
│   │   │   └── Visualizations.jsx
│   │   └── services/
│   │       └── api.js
│   └── package.json
├── requirements.txt
└── README.md
```

## Prerequisites

- Python 3.8 or higher
- Node.js 16 or higher
- npm or yarn

## Installation

### Backend Setup

1. Navigate to the project directory:

```bash
cd data_quality_anomaly_detector
```

2. Create a virtual environment (recommended):

```bash
python -m venv venv
```

3. Activate the virtual environment:

```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

4. Install Python dependencies:

```bash
pip install -r requirements.txt
```

### Frontend Setup

1. Navigate to the frontend directory:

```bash
cd frontend
```

2. Install Node.js dependencies:

```bash
npm install
```

## Usage

### Starting the Backend

1. Make sure you're in the project root directory with the virtual environment activated
2. Start the FastAPI server:

```bash
cd backend
python app.py
```

Or using uvicorn directly:

```bash
uvicorn backend.app:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`

### Starting the Frontend

1. Navigate to the frontend directory:

```bash
cd frontend
```

2. Start the development server:

```bash
npm run dev
```

The frontend will be available at `http://localhost:3000`

## API Endpoints

### `POST /api/upload`

Upload a CSV file for analysis. Accepts any CSV file structure - the system will auto-detect column types and adapt accordingly.

**Request**: Multipart form data with `file` field containing CSV file

**Response**:

```json
{
  "success": true,
  "cache_key": "data_0",
  "summary": {
    "total_rows": 3000,
    "total_columns": 9,
    "missing_values": {...},
    "duplicate_rows": 0,
    "duplicate_claim_ids": 131
  },
  "column_types": {
    "claim_id": "id",
    "claim_amount": "numeric",
    "dob": "date",
    "patient_name": "text"
  },
  "warnings": [],
  "message": "File uploaded and processed successfully. 3000 rows loaded."
}
```

**Note**: The system accepts any CSV structure. If expected columns are missing, warnings will be provided but the file will still be processed.

### `POST /api/detect`

Run anomaly detection on uploaded data.

**Query Parameters**:

- `cache_key` (optional): Key for cached data

**Response**:

```json
{
  "success": true,
  "results": {...},
  "anomaly_count": 450,
  "total_rows": 3000
}
```

### `GET /api/results`

Get cached anomaly detection results.

**Query Parameters**:

- `cache_key` (optional): Key for cached results

### `POST /api/generate-mock-data`

Generate mock insurance payer data with intentional errors.

**Query Parameters**:

- `num_rows` (default: 3000): Number of rows to generate (1000-5000)
- `error_rate` (default: 0.15): Probability of introducing errors (0.0-1.0)

## Data Format

### Flexible Column Structure

The system accepts **any CSV file structure** and automatically adapts to it. It will:

- **Auto-detect column types**: Numeric, date, text, and ID columns
- **Adapt anomaly detection**: Runs appropriate checks based on available columns
- **Provide warnings**: Informs about missing expected columns without failing

### Expected Columns (Optional)

For insurance payer datasets, the following columns are expected but not required:

- `claim_id`: Unique claim identifier (ID column)
- `patient_name`: Patient full name (text column)
- `dob`: Date of birth (date column, YYYY-MM-DD format)
- `zip_code`: 5-digit zip code (text column)
- `claim_date`: Claim date (date column, YYYY-MM-DD format)
- `claim_amount`: Numeric claim amount (numeric column)
- `payer_id`: Payer identifier (text column)
- `diagnosis_code`: Diagnosis code (text column, e.g., ICD10.XX)
- `procedure_code`: Procedure code (text column, e.g., CPTXXXXX)

**The system will work with any CSV file**, detecting column types automatically and running appropriate anomaly detection methods.

## Anomaly Types Detected

The system automatically adapts detection methods based on available columns:

1. **Duplicates**:
   - Detects duplicate ID columns (auto-detects if `claim_id` not present)
   - Detects duplicate entire rows
2. **Missing Values**: Null values in any column
3. **Inconsistencies** (when relevant columns are present):
   - Invalid date formats (for date columns)
   - Invalid zip code formats (for zip_code column)
   - Malformed names (for name columns)
   - Impossible dates (future DOB, claim before DOB)
   - Negative amounts (for numeric amount columns)
4. **Statistical Outliers**: Values beyond Z-score threshold or IQR bounds (for numeric columns)
5. **ML Anomalies**: Patterns detected by Isolation Forest or LOF (for numeric columns)

## Mock Data Generator

The mock data generator creates synthetic insurance payer data with intentional errors. Each generation produces different results (uses time-based random seed).

**Error Distribution**:

- **Duplicates**: ~30% of error rate applied to duplicate claim_ids
- **Missing Values**: ~40% of error rate applied to random fields
- **Inconsistencies**: ~30% of error rate applied to format issues
- **Outliers**: ~20% of error rate applied to extreme numeric values

**Usage**:

- **Via Web UI**: Use the "Generate Mock Data" section with configurable rows (1000-5000) and error rate (0.0-1.0)
- **Via Python**:

```python
from backend.utils.data_generator import generate_mock_data

df = generate_mock_data(num_rows=3000, error_rate=0.15)
```

**Note**: Each generation uses a time-based seed, so the same parameters will produce different data each time for more realistic testing.

## Development

### Running Tests

Currently, the project doesn't include automated tests. You can test the system by:

1. Generating mock data
2. Uploading it through the frontend
3. Running anomaly detection
4. Verifying the results

### Adding New Detection Methods

To add new anomaly detection methods:

1. Add the method to the appropriate module (`statistical.py` or `ml_models.py`)
2. Integrate it into `detector.py`'s `detect_all()` method
3. Update the API response format if needed
4. Update the frontend to display new anomaly types

## Troubleshooting

### Backend Issues

- **Port already in use**: Change the port in `app.py` or kill the process using port 8000
- **Import errors**: Make sure you're running from the correct directory and virtual environment is activated
- **CORS errors**: Check that the frontend URL is in the CORS allowed origins in `app.py`
- **Numpy serialization errors**: Fixed - all numpy types are now converted to native Python types automatically
- **Series boolean errors**: Fixed - all Series evaluations in boolean contexts have been resolved

### Frontend Issues

- **API connection errors**: Ensure the backend is running on port 8000
- **Build errors**: Delete `node_modules` and `package-lock.json`, then run `npm install` again

### Data Upload Issues

- **"Missing required columns" error**: This should no longer occur - the system now accepts any CSV structure
- **Warnings about missing columns**: These are informational only - the system will still process your file
- **No numeric columns warning**: Statistical and ML detection will be limited, but basic checks (duplicates, missing values) will still work

## License

This project is provided as-is for educational and demonstration purposes.

## Recent Updates

- **Flexible Column Structure**: System now accepts any CSV file structure and auto-detects column types
- **Adaptive Detection**: Anomaly detection methods adapt to available columns
- **Improved Error Handling**: Better error messages and warnings instead of strict failures
- **Mock Data Variety**: Mock data generator now produces different results each time
- **Bug Fixes**: Fixed numpy serialization errors and Series boolean evaluation issues

## Contributing

This is a demonstration project. For production use, consider:

- Adding database persistence instead of in-memory caching
- Implementing authentication and authorization
- Adding comprehensive test coverage
- Implementing logging and monitoring
- Adding data export functionality
- Supporting additional file formats (Excel, JSON, etc.)
- Adding column mapping/configuration UI for custom datasets
