"""
Statistical anomaly detection methods: Z-score and IQR (Interquartile Range).
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple


class StatisticalDetector:
    """Statistical methods for anomaly detection."""
    
    def __init__(self, z_score_threshold: float = 3.0, iqr_multiplier: float = 1.5):
        """
        Initialize statistical detector.
        
        Args:
            z_score_threshold: Threshold for Z-score detection (default: 3.0)
            iqr_multiplier: Multiplier for IQR method (default: 1.5)
        """
        self.z_score_threshold = z_score_threshold
        self.iqr_multiplier = iqr_multiplier
    
    def detect_z_score_anomalies(self, df: pd.DataFrame, column: str) -> pd.Series:
        """
        Detect anomalies using Z-score method.
        
        Args:
            df: DataFrame to analyze
            column: Column name to check for anomalies
            
        Returns:
            pandas.Series: Boolean series indicating anomalies (True = anomaly)
        """
        if column not in df.columns:
            return pd.Series([False] * len(df), index=df.index)
        
        # Convert to numeric, handling errors
        numeric_data = pd.to_numeric(df[column], errors='coerce')
        
        # Calculate mean and std, ignoring NaN values
        mean = numeric_data.mean()
        std = numeric_data.std()
        
        # If std is 0 or NaN, no anomalies
        if std == 0 or pd.isna(std):
            return pd.Series([False] * len(df), index=df.index)
        
        # Calculate Z-scores
        z_scores = np.abs((numeric_data - mean) / std)
        
        # Mark anomalies
        anomalies = z_scores > self.z_score_threshold
        
        return anomalies.fillna(False)
    
    def detect_iqr_anomalies(self, df: pd.DataFrame, column: str) -> pd.Series:
        """
        Detect anomalies using IQR (Interquartile Range) method.
        
        Args:
            df: DataFrame to analyze
            column: Column name to check for anomalies
            
        Returns:
            pandas.Series: Boolean series indicating anomalies (True = anomaly)
        """
        if column not in df.columns:
            return pd.Series([False] * len(df), index=df.index)
        
        # Convert to numeric, handling errors
        numeric_data = pd.to_numeric(df[column], errors='coerce')
        
        # Calculate quartiles
        Q1 = numeric_data.quantile(0.25)
        Q3 = numeric_data.quantile(0.75)
        IQR = Q3 - Q1
        
        # If IQR is 0 or NaN, no anomalies
        if IQR == 0 or pd.isna(IQR):
            return pd.Series([False] * len(df), index=df.index)
        
        # Define bounds
        lower_bound = Q1 - self.iqr_multiplier * IQR
        upper_bound = Q3 + self.iqr_multiplier * IQR
        
        # Mark anomalies (values outside bounds)
        anomalies = (numeric_data < lower_bound) | (numeric_data > upper_bound)
        
        return anomalies.fillna(False)
    
    def detect_all_statistical_anomalies(self, df: pd.DataFrame, 
                                        numeric_columns: List[str] = None) -> Dict[str, Any]:
        """
        Detect anomalies in all numeric columns using both Z-score and IQR.
        
        Args:
            df: DataFrame to analyze
            numeric_columns: List of numeric columns to check (default: all numeric columns)
            
        Returns:
            dict: Dictionary with anomaly detection results
        """
        if numeric_columns is None:
            numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        results = {
            'z_score_anomalies': {},
            'iqr_anomalies': {},
            'combined_anomalies': pd.Series([False] * len(df), index=df.index),
            'anomaly_details': []
        }
        
        for column in numeric_columns:
            if column not in df.columns:
                continue
            
            # Z-score detection
            z_anomalies = self.detect_z_score_anomalies(df, column)
            results['z_score_anomalies'][column] = z_anomalies
            
            # IQR detection
            iqr_anomalies = self.detect_iqr_anomalies(df, column)
            results['iqr_anomalies'][column] = iqr_anomalies
            
            # Combined (union of both methods)
            combined = z_anomalies | iqr_anomalies
            results['combined_anomalies'] = results['combined_anomalies'] | combined
            
            # Store details
            num_z_anomalies = z_anomalies.sum()
            num_iqr_anomalies = iqr_anomalies.sum()
            num_combined = combined.sum()
            
            if num_combined > 0:
                results['anomaly_details'].append({
                    'column': column,
                    'method': 'statistical',
                    'z_score_count': int(num_z_anomalies),
                    'iqr_count': int(num_iqr_anomalies),
                    'total_count': int(num_combined),
                    'anomaly_indices': df.index[combined].tolist()
                })
        
        return results
    
    def get_anomaly_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get summary of statistical anomaly detection results.
        
        Args:
            results: Results from detect_all_statistical_anomalies
            
        Returns:
            dict: Summary statistics
        """
        combined_anomalies = results['combined_anomalies']
        total_anomalies = combined_anomalies.sum()
        series_len = len(combined_anomalies)
        
        summary = {
            'total_anomalies': int(total_anomalies),
            'anomaly_rate': float(total_anomalies / series_len) if series_len > 0 else 0.0,
            'columns_checked': len(results['z_score_anomalies']),
            'details': results['anomaly_details']
        }
        
        return summary

