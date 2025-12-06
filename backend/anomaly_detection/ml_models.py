"""
ML-based anomaly detection methods: Isolation Forest and Local Outlier Factor (LOF).
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler


class MLDetector:
    """Machine learning methods for anomaly detection."""
    
    def __init__(self, 
                 contamination: float = 0.1,
                 n_estimators: int = 100,
                 random_state: int = 42):
        """
        Initialize ML detector.
        
        Args:
            contamination: Expected proportion of outliers (default: 0.1)
            n_estimators: Number of trees for Isolation Forest (default: 100)
            random_state: Random seed for reproducibility (default: 42)
        """
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.random_state = random_state
        self.scaler = StandardScaler()
    
    def prepare_features(self, df: pd.DataFrame, 
                        feature_columns: List[str] = None) -> np.ndarray:
        """
        Prepare features for ML models.
        
        Args:
            df: DataFrame to extract features from
            feature_columns: List of columns to use as features (default: all numeric)
            
        Returns:
            numpy.ndarray: Feature matrix
        """
        if feature_columns is None:
            # Use all numeric columns
            feature_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        # Filter to existing columns
        feature_columns = [col for col in feature_columns if col in df.columns]
        
        if len(feature_columns) == 0:
            raise ValueError("No numeric columns found for feature extraction")
        
        # Extract features
        features = df[feature_columns].copy()
        
        # Fill NaN values with median
        features = features.fillna(features.median())
        
        # Convert to numpy array
        feature_array = features.values
        
        return feature_array, feature_columns
    
    def detect_isolation_forest(self, df: pd.DataFrame,
                               feature_columns: List[str] = None) -> Dict[str, Any]:
        """
        Detect anomalies using Isolation Forest.
        
        Args:
            df: DataFrame to analyze
            feature_columns: List of columns to use as features
            
        Returns:
            dict: Detection results
        """
        try:
            # Prepare features
            feature_array, used_columns = self.prepare_features(df, feature_columns)
            
            # Scale features
            feature_array_scaled = self.scaler.fit_transform(feature_array)
            
            # Train Isolation Forest
            iso_forest = IsolationForest(
                contamination=self.contamination,
                n_estimators=self.n_estimators,
                random_state=self.random_state
            )
            
            # Predict anomalies (-1 = anomaly, 1 = normal)
            predictions = iso_forest.fit_predict(feature_array_scaled)
            
            # Convert to boolean (True = anomaly)
            anomalies = predictions == -1
            
            # Get anomaly scores
            scores = iso_forest.score_samples(feature_array_scaled)
            
            results = {
                'anomalies': pd.Series(anomalies, index=df.index),
                'scores': pd.Series(scores, index=df.index),
                'model': 'isolation_forest',
                'feature_columns': used_columns,
                'num_anomalies': int(anomalies.sum())
            }
            
            return results
            
        except Exception as e:
            return {
                'anomalies': pd.Series([False] * len(df), index=df.index),
                'scores': pd.Series([0.0] * len(df), index=df.index),
                'model': 'isolation_forest',
                'error': str(e),
                'num_anomalies': 0
            }
    
    def detect_lof(self, df: pd.DataFrame,
                  feature_columns: List[str] = None,
                  n_neighbors: int = 20) -> Dict[str, Any]:
        """
        Detect anomalies using Local Outlier Factor (LOF).
        
        Args:
            df: DataFrame to analyze
            feature_columns: List of columns to use as features
            n_neighbors: Number of neighbors for LOF (default: 20)
            
        Returns:
            dict: Detection results
        """
        try:
            # Prepare features
            feature_array, used_columns = self.prepare_features(df, feature_columns)
            
            # Scale features
            feature_array_scaled = self.scaler.fit_transform(feature_array)
            
            # Train LOF
            lof = LocalOutlierFactor(
                n_neighbors=min(n_neighbors, len(df) - 1),
                contamination=self.contamination
            )
            
            # Predict anomalies (-1 = anomaly, 1 = normal)
            predictions = lof.fit_predict(feature_array_scaled)
            
            # Convert to boolean (True = anomaly)
            anomalies = predictions == -1
            
            # Get negative outlier factor scores (lower = more anomalous)
            scores = -lof.negative_outlier_factor_
            
            results = {
                'anomalies': pd.Series(anomalies, index=df.index),
                'scores': pd.Series(scores, index=df.index),
                'model': 'lof',
                'feature_columns': used_columns,
                'num_anomalies': int(anomalies.sum())
            }
            
            return results
            
        except Exception as e:
            return {
                'anomalies': pd.Series([False] * len(df), index=df.index),
                'scores': pd.Series([0.0] * len(df), index=df.index),
                'model': 'lof',
                'error': str(e),
                'num_anomalies': 0
            }
    
    def detect_all_ml_anomalies(self, df: pd.DataFrame,
                               feature_columns: List[str] = None) -> Dict[str, Any]:
        """
        Detect anomalies using both Isolation Forest and LOF.
        
        Args:
            df: DataFrame to analyze
            feature_columns: List of columns to use as features
            
        Returns:
            dict: Combined detection results
        """
        # Isolation Forest
        iso_results = self.detect_isolation_forest(df, feature_columns)
        
        # LOF
        lof_results = self.detect_lof(df, feature_columns)
        
        # Combine results (union of both methods)
        combined_anomalies = iso_results['anomalies'] | lof_results['anomalies']
        
        results = {
            'isolation_forest': iso_results,
            'lof': lof_results,
            'combined_anomalies': combined_anomalies,
            'num_combined_anomalies': int(combined_anomalies.sum())
        }
        
        return results
    
    def get_anomaly_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get summary of ML anomaly detection results.
        
        Args:
            results: Results from detect_all_ml_anomalies
            
        Returns:
            dict: Summary statistics
        """
        iso_anomalies = results['isolation_forest']['num_anomalies']
        lof_anomalies = results['lof']['num_anomalies']
        combined = results['num_combined_anomalies']
        
        total_rows = len(results['combined_anomalies'])
        
        summary = {
            'isolation_forest_anomalies': iso_anomalies,
            'lof_anomalies': lof_anomalies,
            'combined_anomalies': combined,
            'anomaly_rate': float(combined / total_rows) if total_rows > 0 else 0.0,
            'feature_columns': results['isolation_forest'].get('feature_columns', [])
        }
        
        return summary

