"""
Data transformation module for cleaning and standardizing data.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any
import re


class DataTransformer:
    """Transforms and cleans data for analysis."""
    
    def __init__(self):
        self.date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']
        self.zip_pattern = re.compile(r'^\d{5}$')
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and clean the dataframe.
        
        Args:
            df: Raw dataframe to transform
            
        Returns:
            pandas.DataFrame: Transformed dataframe
        """
        df = df.copy()
        
        # Standardize date formats
        df = self._standardize_dates(df)
        
        # Standardize zip codes
        df = self._standardize_zip_codes(df)
        
        # Standardize names
        df = self._standardize_names(df)
        
        # Convert claim_amount to numeric
        df = self._standardize_numeric(df)
        
        # Clean whitespace
        df = self._clean_whitespace(df)
        
        return df
    
    def _standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize date formats to YYYY-MM-DD."""
        date_columns = ['dob', 'claim_date']
        
        for col in date_columns:
            if col not in df.columns:
                continue
            
            df[col] = df[col].apply(lambda x: self._parse_date(x) if pd.notna(x) else x)
        
        return df
    
    def _parse_date(self, date_str: Any) -> str:
        """Parse date string to standard format."""
        if pd.isna(date_str):
            return np.nan
        
        # Try different date formats
        for fmt in self.date_formats:
            try:
                dt = datetime.strptime(str(date_str), fmt)
                return dt.strftime('%Y-%m-%d')
            except:
                continue
        
        # If all formats fail, return original (will be flagged as anomaly)
        return date_str
    
    def _standardize_zip_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize zip code format to 5-digit string."""
        if 'zip_code' not in df.columns:
            return df
        
        def clean_zip(zip_val):
            if pd.isna(zip_val):
                return np.nan
            
            zip_str = str(zip_val).strip()
            # Remove non-numeric characters
            zip_str = re.sub(r'\D', '', zip_str)
            
            # If it's 5 digits, return it
            if len(zip_str) == 5:
                return zip_str
            
            # If it's longer, take first 5
            if len(zip_str) > 5:
                return zip_str[:5]
            
            # If it's shorter, pad with zeros
            if len(zip_str) < 5:
                return zip_str.zfill(5)
            
            return zip_str
        
        df['zip_code'] = df['zip_code'].apply(clean_zip)
        return df
    
    def _standardize_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize name format (title case, remove extra spaces)."""
        if 'patient_name' not in df.columns:
            return df
        
        def clean_name(name):
            if pd.isna(name) or name == '':
                return np.nan
            
            name_str = str(name).strip()
            # Remove special characters except spaces and hyphens
            name_str = re.sub(r'[^a-zA-Z\s\-]', '', name_str)
            # Title case
            name_str = name_str.title()
            # Remove extra spaces
            name_str = ' '.join(name_str.split())
            return name_str if name_str else np.nan
        
        df['patient_name'] = df['patient_name'].apply(clean_name)
        return df
    
    def _standardize_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert numeric columns to proper types."""
        if 'claim_amount' in df.columns:
            df['claim_amount'] = pd.to_numeric(df['claim_amount'], errors='coerce')
        
        return df
    
    def _clean_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove leading/trailing whitespace from string columns."""
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        return df
    
    def get_transformation_summary(self, original_df: pd.DataFrame, transformed_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary of transformations applied.
        
        Args:
            original_df: Original dataframe
            transformed_df: Transformed dataframe
            
        Returns:
            dict: Summary of transformations
        """
        return {
            'rows_processed': len(transformed_df),
            'columns_processed': len(transformed_df.columns),
            'missing_values_before': original_df.isnull().sum().to_dict(),
            'missing_values_after': transformed_df.isnull().sum().to_dict()
        }

