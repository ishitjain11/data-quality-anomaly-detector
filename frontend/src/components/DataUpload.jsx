import React, { useState } from 'react';
import { uploadFile, generateMockData } from '../services/api';

function DataUpload({ onUploadSuccess, onGenerateMockData, loading }) {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [mockDataRows, setMockDataRows] = useState(3000);
  const [mockDataErrorRate, setMockDataErrorRate] = useState(0.15);

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      if (selectedFile.type !== 'text/csv' && !selectedFile.name.endsWith('.csv')) {
        setError('Please upload a CSV file');
        return;
      }
      setFile(selectedFile);
      setError(null);
      setSuccess(null);
    }
  };

  const handleUpload = async () => {
    if (!file) {
      setError('Please select a file');
      return;
    }

    setUploading(true);
    setError(null);
    setSuccess(null);

    try {
      const response = await uploadFile(file);
      setSuccess(response.message);
      onUploadSuccess(response);
    } catch (err) {
      setError(err.response?.data?.detail || 'Error uploading file');
    } finally {
      setUploading(false);
    }
  };

  const handleGenerateMock = async () => {
    if (mockDataRows < 1000 || mockDataRows > 5000) {
      setError('Number of rows must be between 1000 and 5000');
      return;
    }

    if (mockDataErrorRate < 0 || mockDataErrorRate > 1) {
      setError('Error rate must be between 0 and 1');
      return;
    }

    setError(null);
    setSuccess(null);
    await onGenerateMockData(mockDataRows, mockDataErrorRate);
  };

  return (
    <div>
      <h2>Upload Data</h2>
      
      <div style={{ marginBottom: '20px' }}>
        <label style={{ display: 'block', marginBottom: '10px', fontWeight: '600' }}>
          Upload CSV File:
        </label>
        <input
          type="file"
          accept=".csv"
          onChange={handleFileChange}
          disabled={uploading || loading}
          style={{ marginBottom: '10px' }}
        />
        <button
          className="button"
          onClick={handleUpload}
          disabled={!file || uploading || loading}
        >
          {uploading ? 'Uploading...' : 'Upload File'}
        </button>
      </div>

      <div style={{ 
        borderTop: '1px solid #ddd', 
        paddingTop: '20px', 
        marginTop: '20px' 
      }}>
        <h3 style={{ marginBottom: '15px' }}>Or Generate Mock Data</h3>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '15px', marginBottom: '15px' }}>
          <div>
            <label style={{ display: 'block', marginBottom: '5px', fontSize: '14px' }}>
              Number of Rows (1000-5000):
            </label>
            <input
              type="number"
              className="input"
              value={mockDataRows}
              onChange={(e) => setMockDataRows(parseInt(e.target.value) || 3000)}
              min={1000}
              max={5000}
              disabled={loading}
              placeholder="3000"
            />
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: '5px', fontSize: '14px' }}>
              Error Rate (0.0-1.0):
            </label>
            <input
              type="number"
              className="input"
              value={mockDataErrorRate}
              onChange={(e) => setMockDataErrorRate(parseFloat(e.target.value) || 0.15)}
              min={0}
              max={1}
              step={0.01}
              disabled={loading}
              placeholder="0.15"
            />
          </div>
        </div>
        <button
          className="button button-secondary"
          onClick={handleGenerateMock}
          disabled={loading}
        >
          {loading ? 'Generating...' : 'Generate Mock Data'}
        </button>
      </div>

      {error && <div className="error" style={{ marginTop: '15px' }}>{error}</div>}
      {success && <div className="success" style={{ marginTop: '15px' }}>{success}</div>}
    </div>
  );
}

export default DataUpload;

