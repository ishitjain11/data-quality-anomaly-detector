"""
Data extraction module for reading and validating CSV files.
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
import io


class DataExtractor:
    """Extracts data from CSV files and validates format."""
    
    def __init__(self):
        # Expected columns (for reference, but not strictly required)
        self.expected_columns = [
            'claim_id', 'patient_name', 'dob', 'zip_code', 
            'claim_date', 'claim_amount', 'payer_id', 
            'diagnosis_code', 'procedure_code'
        ]
    
    def extract_from_file(self, file_path: str) -> pd.DataFrame:
        """
        Extract data from a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            pandas.DataFrame: Extracted data
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file format is invalid
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {str(e)}")
        
        # Check if dataframe is empty
        if df.empty:
            raise ValueError("CSV file is empty")
        
        # Log available columns for debugging (not an error)
        missing_expected = set(self.expected_columns) - set(df.columns)
        if missing_expected:
            print(f"Info: Expected columns not found: {missing_expected}")
            print(f"Info: Available columns: {list(df.columns)}")
        
        return df
    
    def extract_from_bytes(self, file_bytes: bytes, filename: str = "uploaded.csv") -> pd.DataFrame:
        """
        Extract data from uploaded file bytes.
        
        Args:
            file_bytes: File content as bytes
            filename: Original filename (for error messages)
            
        Returns:
            pandas.DataFrame: Extracted data
            
        Raises:
            ValueError: If file format is invalid
        """
        try:
            df = pd.read_csv(io.BytesIO(file_bytes))
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {str(e)}")
        
        # Check if dataframe is empty
        if df.empty:
            raise ValueError("CSV file is empty")
        
        # Log available columns for debugging (not an error)
        missing_expected = set(self.expected_columns) - set(df.columns)
        if missing_expected:
            print(f"Info: Expected columns not found: {missing_expected}")
            print(f"Info: Available columns: {list(df.columns)}")
        
        return df
    
    def detect_column_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Auto-detect column types (numeric, date, text, id).
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            dict: Column name -> type mapping
        """
        column_types = {}
        
        for col in df.columns:
            # Try to detect numeric columns
            numeric_data = pd.to_numeric(df[col], errors='coerce')
            if len(df) > 0 and numeric_data.notna().sum() / len(df) > 0.8:  # 80% numeric
                column_types[col] = 'numeric'
                continue
            
            # Try to detect date columns
            date_data = pd.to_datetime(df[col], errors='coerce')
            if len(df) > 0 and date_data.notna().sum() / len(df) > 0.5:  # 50% parseable as date
                column_types[col] = 'date'
                continue
            
            # Check for ID-like columns (unique identifiers)
            if len(df) > 0 and df[col].nunique() / len(df) > 0.9:  # 90% unique values
                column_types[col] = 'id'
                continue
            
            # Default to text
            column_types[col] = 'text'
        
        return column_types
    
    def validate_format(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate the format of extracted data (flexible validation).
        
        Args:
            df: DataFrame to validate
            
        Returns:
            dict: Validation results with status and issues
        """
        issues = []
        warnings = []
        
        # Check for empty dataframe
        if df.empty:
            issues.append("DataFrame is empty")
            return {
                'valid': False,
                'issues': issues,
                'warnings': warnings,
                'row_count': 0,
                'column_count': 0
            }
        
        # Check for minimum data requirements
        if len(df) < 10:
            warnings.append("Very few rows (< 10) - results may not be reliable")
        
        # Detect column types
        column_types = self.detect_column_types(df)
        
        # Check for at least one numeric column for statistical/ML detection
        numeric_cols = [col for col, col_type in column_types.items() if col_type == 'numeric']
        if len(numeric_cols) == 0:
            warnings.append("No numeric columns detected - statistical and ML anomaly detection will be limited")
        
        # Check for potential ID column
        id_cols = [col for col, col_type in column_types.items() if col_type == 'id']
        if len(id_cols) == 0:
            warnings.append("No ID column detected - duplicate detection will check entire rows")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'warnings': warnings,
            'row_count': len(df),
            'column_count': len(df.columns),
            'column_types': column_types,
            'numeric_columns': numeric_cols,
            'id_columns': id_cols
        }

