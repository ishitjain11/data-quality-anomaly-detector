"""
Main anomaly detection orchestrator that combines all detection methods.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from .statistical import StatisticalDetector
from .ml_models import MLDetector


class AnomalyDetector:
    """Orchestrates all anomaly detection methods and aggregates results."""
    
    def __init__(self, 
                 z_score_threshold: float = 3.0,
                 iqr_multiplier: float = 1.5,
                 ml_contamination: float = 0.1):
        """
        Initialize anomaly detector.
        
        Args:
            z_score_threshold: Threshold for Z-score detection
            iqr_multiplier: Multiplier for IQR method
            ml_contamination: Expected proportion of outliers for ML methods
        """
        self.statistical_detector = StatisticalDetector(
            z_score_threshold=z_score_threshold,
            iqr_multiplier=iqr_multiplier
        )
        self.ml_detector = MLDetector(contamination=ml_contamination)
    
    def detect_duplicates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect duplicate records (adapts to available columns).
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            dict: Duplicate detection results
        """
        results = {
            'duplicate_rows': df.duplicated().sum(),
            'duplicate_ids': {},
            'duplicate_indices': []
        }
        
        # Find potential ID columns (high uniqueness) or use claim_id if available
        id_columns = []
        if 'claim_id' in df.columns:
            id_columns.append('claim_id')
        else:
            # Auto-detect ID columns (high uniqueness)
            for col in df.columns:
                if len(df) > 0:
                    uniqueness_ratio = df[col].nunique() / len(df)
                    if uniqueness_ratio > 0.8:  # 80% unique - likely an ID column
                        id_columns.append(col)
        
        # Check duplicates for each ID column
        for col in id_columns:
            duplicate_mask = df[col].duplicated(keep=False)
            if duplicate_mask.any():
                results['duplicate_ids'][col] = {
                    'count': int(duplicate_mask.sum()),
                    'indices': df.index[duplicate_mask].tolist()
                }
                results['duplicate_indices'].extend(df.index[duplicate_mask].tolist())
        
        # Remove duplicates from indices list
        results['duplicate_indices'] = list(set(results['duplicate_indices']))
        
        # For backward compatibility, also include claim_id count if it exists
        if 'claim_id' in df.columns:
            results['duplicate_claim_ids'] = results['duplicate_ids'].get('claim_id', {}).get('count', 0)
        else:
            # Sum all duplicate IDs if no claim_id
            results['duplicate_claim_ids'] = sum([v.get('count', 0) for v in results['duplicate_ids'].values()])
        
        return results
    
    def detect_missing_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect missing values.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            dict: Missing value detection results
        """
        missing_counts = df.isnull().sum()
        missing_percentages = (missing_counts / len(df)) * 100
        
        # Find rows with missing values
        rows_with_missing = df.isnull().any(axis=1)
        
        results = {
            'missing_counts': missing_counts.to_dict(),
            'missing_percentages': missing_percentages.to_dict(),
            'rows_with_missing': rows_with_missing.sum(),
            'missing_indices': df.index[rows_with_missing].tolist(),
            'columns_with_missing': missing_counts[missing_counts > 0].index.tolist()
        }
        
        return results
    
    def detect_inconsistencies(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect data inconsistencies (invalid formats, impossible values).
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            dict: Inconsistency detection results
        """
        inconsistencies = []
        inconsistent_indices = set()
        
        # Check date formats
        date_columns = ['dob', 'claim_date']
        for col in date_columns:
            if col in df.columns:
                # Check if dates are parseable
                parsed_dates = pd.to_datetime(df[col], errors='coerce')
                invalid_dates = parsed_dates.isna() & df[col].notna()
                
                if invalid_dates.any():
                    count = invalid_dates.sum()
                    inconsistencies.append({
                        'column': col,
                        'type': 'invalid_date_format',
                        'count': int(count),
                        'indices': df.index[invalid_dates].tolist()
                    })
                    inconsistent_indices.update(df.index[invalid_dates])
        
        # Check zip code format (should be 5 digits)
        if 'zip_code' in df.columns:
            zip_valid = df['zip_code'].apply(
                lambda x: pd.isna(x) or (isinstance(x, str) and x.isdigit() and len(x) == 5)
            )
            invalid_zips = ~zip_valid & df['zip_code'].notna()
            
            if invalid_zips.any():
                count = invalid_zips.sum()
                inconsistencies.append({
                    'column': 'zip_code',
                    'type': 'invalid_zip_format',
                    'count': int(count),
                    'indices': df.index[invalid_zips].tolist()
                })
                inconsistent_indices.update(df.index[invalid_zips])
        
        # Check name format (should be alphabetic with spaces)
        if 'patient_name' in df.columns:
            name_valid = df['patient_name'].apply(
                lambda x: pd.isna(x) or (
                    isinstance(x, str) and 
                    len(x) > 1 and 
                    x.replace(' ', '').replace('-', '').isalpha()
                )
            )
            invalid_names = ~name_valid & df['patient_name'].notna()
            
            if invalid_names.any():
                count = invalid_names.sum()
                inconsistencies.append({
                    'column': 'patient_name',
                    'type': 'invalid_name_format',
                    'count': int(count),
                    'indices': df.index[invalid_names].tolist()
                })
                inconsistent_indices.update(df.index[invalid_names])
        
        # Check for impossible dates (DOB in future, claim date before DOB)
        if 'dob' in df.columns and 'claim_date' in df.columns:
            dob_dates = pd.to_datetime(df['dob'], errors='coerce')
            claim_dates = pd.to_datetime(df['claim_date'], errors='coerce')
            
            # DOB in future
            future_dob = dob_dates > pd.Timestamp.now()
            if future_dob.any():
                count = future_dob.sum()
                inconsistencies.append({
                    'column': 'dob',
                    'type': 'future_dob',
                    'count': int(count),
                    'indices': df.index[future_dob].tolist()
                })
                inconsistent_indices.update(df.index[future_dob])
            
            # Claim date before DOB
            invalid_order = (claim_dates < dob_dates) & dob_dates.notna() & claim_dates.notna()
            if invalid_order.any():
                count = invalid_order.sum()
                inconsistencies.append({
                    'column': 'claim_date',
                    'type': 'claim_before_dob',
                    'count': int(count),
                    'indices': df.index[invalid_order].tolist()
                })
                inconsistent_indices.update(df.index[invalid_order])
        
        # Check for negative claim amounts
        if 'claim_amount' in df.columns:
            negative_amounts = df['claim_amount'] < 0
            if negative_amounts.any():
                count = negative_amounts.sum()
                inconsistencies.append({
                    'column': 'claim_amount',
                    'type': 'negative_amount',
                    'count': int(count),
                    'indices': df.index[negative_amounts].tolist()
                })
                inconsistent_indices.update(df.index[negative_amounts])
        
        results = {
            'inconsistencies': inconsistencies,
            'total_inconsistent_rows': len(inconsistent_indices),
            'inconsistent_indices': list(inconsistent_indices)
        }
        
        return results
    
    def detect_all(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Run all anomaly detection methods and aggregate results.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            dict: Comprehensive anomaly detection results
        """
        results = {
            'duplicates': self.detect_duplicates(df),
            'missing_values': self.detect_missing_values(df),
            'inconsistencies': self.detect_inconsistencies(df),
            'statistical': {},
            'ml': {},
            'summary': {}
        }
        
        # Statistical detection (only on numeric columns)
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(numeric_columns) > 0:
            stat_results = self.statistical_detector.detect_all_statistical_anomalies(df, numeric_columns)
            results['statistical'] = stat_results
            results['statistical']['summary'] = self.statistical_detector.get_anomaly_summary(stat_results)
        
        # ML detection
        if len(numeric_columns) > 0:
            ml_results = self.ml_detector.detect_all_ml_anomalies(df, numeric_columns)
            results['ml'] = ml_results
            results['ml']['summary'] = self.ml_detector.get_anomaly_summary(ml_results)
        
        # Aggregate all anomaly indices
        all_anomaly_indices = set()
        
        # Add duplicate indices (handle both old and new structure)
        duplicate_indices = results['duplicates'].get('duplicate_indices', [])
        if duplicate_indices:
            all_anomaly_indices.update(duplicate_indices)
        
        # Add missing value indices
        all_anomaly_indices.update(results['missing_values']['missing_indices'])
        
        # Add inconsistency indices
        all_anomaly_indices.update(results['inconsistencies']['inconsistent_indices'])
        
        # Add statistical anomaly indices
        stat_dict = results.get('statistical', {})
        if isinstance(stat_dict, dict):
            if 'combined_anomalies' in stat_dict:
                stat_anomalies = stat_dict['combined_anomalies']
                if isinstance(stat_anomalies, pd.Series):
                    # Convert boolean Series to list of indices
                    anomaly_indices = df.index[stat_anomalies].tolist()
                    all_anomaly_indices.update(anomaly_indices)
        
        # Add ML anomaly indices
        ml_dict = results.get('ml', {})
        if isinstance(ml_dict, dict):
            if 'combined_anomalies' in ml_dict:
                ml_anomalies = ml_dict['combined_anomalies']
                if isinstance(ml_anomalies, pd.Series):
                    # Convert boolean Series to list of indices
                    anomaly_indices = df.index[ml_anomalies].tolist()
                    all_anomaly_indices.update(anomaly_indices)
        
        # Create overall summary with safe access
        stat_summary = {}
        if isinstance(stat_dict, dict):
            temp_summary = stat_dict.get('summary', {})
            if isinstance(temp_summary, dict):
                stat_summary = temp_summary
        
        ml_summary = {}
        if isinstance(ml_dict, dict):
            temp_summary = ml_dict.get('summary', {})
            if isinstance(temp_summary, dict):
                ml_summary = temp_summary
        
        # Safely extract count values, ensuring they're integers not Series
        stat_count = 0
        if isinstance(stat_summary, dict):
            stat_val = stat_summary.get('total_anomalies', 0)
            # Check type first to avoid Series evaluation in boolean context
            if isinstance(stat_val, pd.Series):
                stat_count = int(stat_val.sum())
            elif isinstance(stat_val, (int, np.integer)):
                stat_count = int(stat_val)
            else:
                try:
                    stat_count = int(stat_val) if stat_val is not None else 0
                except (TypeError, ValueError):
                    stat_count = 0
        
        ml_count = 0
        if isinstance(ml_summary, dict):
            ml_val = ml_summary.get('combined_anomalies', 0)
            # Check type first to avoid Series evaluation in boolean context
            if isinstance(ml_val, pd.Series):
                ml_count = int(ml_val.sum())
            elif isinstance(ml_val, (int, np.integer)):
                ml_count = int(ml_val)
            else:
                try:
                    ml_count = int(ml_val) if ml_val is not None else 0
                except (TypeError, ValueError):
                    ml_count = 0
        
        # Calculate duplicate count (handle both old and new structure)
        duplicate_count = 0
        if 'duplicate_claim_ids' in results['duplicates']:
            duplicate_count = results['duplicates']['duplicate_claim_ids']
        elif 'duplicate_ids' in results['duplicates']:
            duplicate_count = sum([v.get('count', 0) for v in results['duplicates']['duplicate_ids'].values()])
        
        results['summary'] = {
            'total_rows': len(df),
            'total_anomalies': len(all_anomaly_indices),
            'anomaly_rate': len(all_anomaly_indices) / len(df) if len(df) > 0 else 0.0,
            'anomaly_indices': list(all_anomaly_indices),
            'duplicate_count': duplicate_count,
            'missing_value_count': results['missing_values']['rows_with_missing'],
            'inconsistency_count': results['inconsistencies']['total_inconsistent_rows'],
            'statistical_anomaly_count': stat_count,
            'ml_anomaly_count': ml_count
        }
        
        return results
    
    def get_anomaly_records(self, df: pd.DataFrame, results: Dict[str, Any]) -> pd.DataFrame:
        """
        Get DataFrame containing only the anomalous records.
        
        Args:
            df: Original DataFrame
            results: Detection results from detect_all()
            
        Returns:
            pandas.DataFrame: DataFrame with only anomalous records
        """
        anomaly_indices = results['summary']['anomaly_indices']
        return df.loc[anomaly_indices].copy()

