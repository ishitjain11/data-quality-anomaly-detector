"""
Mock data generator for insurance payer datasets with intentional errors.
Generates 1k-5k rows with duplicates, missing values, inconsistencies, and outliers.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from pathlib import Path
import time


def generate_mock_data(num_rows=3000, error_rate=0.15, output_path=None):
    """
    Generate synthetic insurance payer data with intentional errors.
    
    Args:
        num_rows: Number of rows to generate (default 3000, between 1k-5k)
        error_rate: Probability of introducing errors (0.0 to 1.0)
        output_path: Path to save CSV file (default: backend/data/mock_data.csv)
    
    Returns:
        pandas.DataFrame: Generated data with intentional errors
    """
    if output_path is None:
        output_path = Path(__file__).parent.parent / "data" / "mock_data.csv"
    else:
        # Ensure output_path is a Path object
        output_path = Path(output_path)
    
    # Use current time as seed for different results each time
    current_seed = int(time.time() * 1000) % (2**31)
    np.random.seed(current_seed)
    random.seed(current_seed)
    
    # Generate base data
    data = []
    claim_ids = set()
    base_date = datetime(2020, 1, 1)
    
    # Common names for realistic data
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Jessica",
                   "William", "Ashley", "James", "Amanda", "Christopher", "Melissa", "Daniel"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas"]
    
    # Generate rows
    for i in range(num_rows):
        # Generate unique claim_id (but we'll introduce duplicates later)
        claim_id = f"CLM{str(i+1).zfill(6)}"
        
        # Generate patient name
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        name = f"{first_name} {last_name}"
        
        # Generate date of birth (between 18-80 years ago)
        years_ago = random.randint(18, 80)
        dob = base_date - timedelta(days=years_ago * 365 + random.randint(0, 365))
        
        # Generate claim date (within last 2 years)
        days_ago = random.randint(0, 730)
        claim_date = base_date + timedelta(days=days_ago)
        
        # Generate zip code (5-digit)
        zip_code = f"{random.randint(10000, 99999)}"
        
        # Generate claim amount (normal distribution with some outliers)
        base_amount = np.random.normal(5000, 2000)
        claim_amount = max(100, round(base_amount, 2))
        
        # Generate payer ID
        payer_id = f"PAY{random.randint(1, 10):03d}"
        
        # Generate diagnosis code
        diagnosis_code = f"ICD{random.randint(10, 99)}.{random.randint(10, 99)}"
        
        # Generate procedure code
        procedure_code = f"CPT{random.randint(10000, 99999)}"
        
        data.append({
            'claim_id': claim_id,
            'patient_name': name,
            'dob': dob.strftime('%Y-%m-%d'),
            'zip_code': zip_code,
            'claim_date': claim_date.strftime('%Y-%m-%d'),
            'claim_amount': claim_amount,
            'payer_id': payer_id,
            'diagnosis_code': diagnosis_code,
            'procedure_code': procedure_code
        })
    
    df = pd.DataFrame(data)
    
    # Introduce intentional errors
    
    # 1. Duplicates: Same claim_id appearing multiple times
    num_duplicates = int(num_rows * error_rate * 0.3)
    duplicate_indices = random.sample(range(num_rows), num_duplicates)
    for idx in duplicate_indices:
        # Copy a random row and reuse its claim_id
        source_idx = random.randint(0, num_rows - 1)
        df.loc[idx, 'claim_id'] = df.loc[source_idx, 'claim_id']
    
    # 2. Missing values: Null values for zip_code, dob, name, etc.
    num_missing = int(num_rows * error_rate * 0.4)
    missing_indices = random.sample(range(num_rows), num_missing)
    for idx in missing_indices:
        field = random.choice(['zip_code', 'dob', 'patient_name', 'payer_id', 'diagnosis_code'])
        df.loc[idx, field] = np.nan
    
    # 3. Inconsistencies: Invalid date formats, invalid zip codes, malformed names
    num_inconsistencies = int(num_rows * error_rate * 0.3)
    inconsistency_indices = random.sample(range(num_rows), num_inconsistencies)
    for idx in inconsistency_indices:
        error_type = random.choice(['date_format', 'zip_format', 'name_format', 'invalid_date'])
        
        if error_type == 'date_format':
            # Invalid date format
            df.loc[idx, 'dob'] = f"{random.randint(1, 12)}/{random.randint(1, 31)}/{random.randint(1900, 2020)}"
        elif error_type == 'zip_format':
            # Invalid zip code format (too short, too long, or non-numeric)
            df.loc[idx, 'zip_code'] = random.choice(['123', '123456789', 'ABCDE', '12-34'])
        elif error_type == 'name_format':
            # Malformed name (numbers, special chars, or empty)
            df.loc[idx, 'patient_name'] = random.choice(['12345', 'John@Doe', 'A', ''])
        elif error_type == 'invalid_date':
            # Invalid date (future DOB, impossible dates)
            df.loc[idx, 'dob'] = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
    
    # 4. Outliers: Extreme values in numeric fields (claim_amount)
    num_outliers = int(num_rows * error_rate * 0.2)
    outlier_indices = random.sample(range(num_rows), num_outliers)
    for idx in outlier_indices:
        # Extreme claim amounts (very high or negative)
        if random.random() > 0.5:
            df.loc[idx, 'claim_amount'] = random.uniform(100000, 1000000)  # Very high
        else:
            df.loc[idx, 'claim_amount'] = random.uniform(-10000, -100)  # Negative
    
    # Save to CSV
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"Failed to create directory for output file: {output_path.parent}. Error: {str(e)}")
    
    try:
        df.to_csv(output_path, index=False)
    except Exception as e:
        raise Exception(f"Failed to write CSV file to {output_path}. Error: {str(e)}")
    
    return df


if __name__ == "__main__":
    # Generate mock data when run directly
    df = generate_mock_data(num_rows=3000, error_rate=0.15)
    print(f"Generated {len(df)} rows of mock data")
    print(f"\nData summary:")
    print(df.head(10))
    print(f"\nMissing values:")
    print(df.isnull().sum())

