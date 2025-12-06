"""
Data loading module for preparing data for analysis.
"""

import pandas as pd
from typing import Dict, Any, Optional
import numpy as np


class DataLoader:
    """Prepares and loads data for anomaly detection analysis."""
    
    def __init__(self):
        self.data_cache = {}
    
    def prepare_for_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare dataframe for anomaly detection analysis.
        
        Args:
            df: Transformed dataframe
            
        Returns:
            pandas.DataFrame: Prepared dataframe ready for analysis
        """
        df = df.copy()
        
        # Ensure proper data types
        df = self._ensure_data_types(df)
        
        # Add derived features that might be useful for anomaly detection
        df = self._add_derived_features(df)
        
        return df
    
    def _ensure_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure correct data types for analysis."""
        # Ensure claim_amount is numeric
        if 'claim_amount' in df.columns:
            df['claim_amount'] = pd.to_numeric(df['claim_amount'], errors='coerce')
        
        # Ensure dates are datetime objects
        date_columns = ['dob', 'claim_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
    
    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived features that might help with anomaly detection."""
        # Age from DOB
        if 'dob' in df.columns and 'claim_date' in df.columns:
            df['age'] = (df['claim_date'] - df['dob']).dt.days / 365.25
            df['age'] = df['age'].round(1)
        
        # Days since claim
        if 'claim_date' in df.columns:
            df['days_since_claim'] = (pd.Timestamp.now() - df['claim_date']).dt.days
        
        return df
    
    def cache_data(self, key: str, df: pd.DataFrame) -> None:
        """
        Cache dataframe for later retrieval.
        
        Args:
            key: Cache key
            df: DataFrame to cache
        """
        self.data_cache[key] = df.copy()
    
    def get_cached_data(self, key: str) -> Optional[pd.DataFrame]:
        """
        Retrieve cached dataframe.
        
        Args:
            key: Cache key
            
        Returns:
            pandas.DataFrame or None: Cached dataframe if exists
        """
        return self.data_cache.get(key)
    
    def clear_cache(self) -> None:
        """Clear all cached data."""
        self.data_cache.clear()
    
    def get_data_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics of the data.
        
        Args:
            df: DataFrame to summarize
            
        Returns:
            dict: Summary statistics
        """
        # Calculate duplicate claim IDs (adapt to available columns)
        duplicate_claim_ids = 0
        if 'claim_id' in df.columns:
            duplicate_claim_ids = df['claim_id'].duplicated().sum()
        else:
            # Try to find ID-like columns and check for duplicates
            for col in df.columns:
                if len(df) > 0 and df[col].nunique() / len(df) > 0.8:  # High uniqueness = likely ID
                    duplicate_claim_ids = df[col].duplicated().sum()
                    break
        
        summary = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum(),
            'duplicate_claim_ids': duplicate_claim_ids
        }
        
        # Numeric column statistics
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            summary['numeric_statistics'] = df[numeric_cols].describe().to_dict()
        
        return summary

