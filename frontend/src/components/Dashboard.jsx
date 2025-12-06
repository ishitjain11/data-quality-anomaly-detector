import React, { useState, useEffect } from 'react';
import DataUpload from './DataUpload';
import AnomalyTable from './AnomalyTable';
import Visualizations from './Visualizations';
import { detectAnomalies, getResults, generateMockData } from '../services/api';

function Dashboard() {
  const [cacheKey, setCacheKey] = useState(null);
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [summary, setSummary] = useState(null);

  const handleUploadSuccess = (data) => {
    setCacheKey(data.cache_key);
    setSummary(data.summary);
    setError(null);
  };

  const handleDetectAnomalies = async () => {
    if (!cacheKey) {
      setError('Please upload a file first');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await detectAnomalies(cacheKey);
      setResults(response.results);
    } catch (err) {
      setError(err.response?.data?.detail || 'Error detecting anomalies');
    } finally {
      setLoading(false);
    }
  };

  const handleGenerateMockData = async (numRows, errorRate) => {
    setLoading(true);
    setError(null);

    try {
      const response = await generateMockData(numRows, errorRate);
      setCacheKey(response.cache_key);
      setSummary(response.summary);
      
      // Automatically run detection on mock data
      const detectResponse = await detectAnomalies(response.cache_key);
      setResults(detectResponse.results);
    } catch (err) {
      setError(err.response?.data?.detail || 'Error generating mock data');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container">
      <h1 style={{ marginBottom: '30px', color: '#333' }}>
        Data Quality Anomaly Detector
      </h1>

      {error && <div className="error">{error}</div>}

      <div className="card">
        <DataUpload
          onUploadSuccess={handleUploadSuccess}
          onGenerateMockData={handleGenerateMockData}
          loading={loading}
        />
      </div>

      {summary && (
        <div className="card">
          <h2>Data Summary</h2>
          <div className="stats-grid">
            <div className="stat-card">
              <div className="stat-value">{summary.total_rows}</div>
              <div className="stat-label">Total Rows</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">{summary.duplicate_claim_ids}</div>
              <div className="stat-label">Duplicate Claim IDs</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">
                {Object.values(summary.missing_values).reduce((a, b) => a + b, 0)}
              </div>
              <div className="stat-label">Missing Values</div>
            </div>
          </div>
        </div>
      )}

      {cacheKey && !results && (
        <div className="card">
          <button
            className="button"
            onClick={handleDetectAnomalies}
            disabled={loading}
          >
            {loading ? 'Detecting Anomalies...' : 'Detect Anomalies'}
          </button>
        </div>
      )}

      {results && (
        <>
          <div className="card">
            <h2>Detection Summary</h2>
            <div className="stats-grid">
              <div className="stat-card">
                <div className="stat-value">{results.summary?.total_anomalies || 0}</div>
                <div className="stat-label">Total Anomalies</div>
              </div>
              <div className="stat-card">
                <div className="stat-value">
                  {((results.summary?.anomaly_rate || 0) * 100).toFixed(2)}%
                </div>
                <div className="stat-label">Anomaly Rate</div>
              </div>
              <div className="stat-card">
                <div className="stat-value">{results.summary?.duplicate_count || 0}</div>
                <div className="stat-label">Duplicates</div>
              </div>
              <div className="stat-card">
                <div className="stat-value">{results.summary?.missing_value_count || 0}</div>
                <div className="stat-label">Missing Values</div>
              </div>
              <div className="stat-card">
                <div className="stat-value">{results.summary?.inconsistency_count || 0}</div>
                <div className="stat-label">Inconsistencies</div>
              </div>
              <div className="stat-card">
                <div className="stat-value">{results.summary?.statistical_anomaly_count || 0}</div>
                <div className="stat-label">Statistical Anomalies</div>
              </div>
              <div className="stat-card">
                <div className="stat-value">{results.summary?.ml_anomaly_count || 0}</div>
                <div className="stat-label">ML Anomalies</div>
              </div>
            </div>
          </div>

          <div className="card">
            <Visualizations results={results} />
          </div>

          <div className="card">
            <AnomalyTable results={results} />
          </div>
        </>
      )}
    </div>
  );
}

export default Dashboard;

