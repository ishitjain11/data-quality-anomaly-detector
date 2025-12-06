import React, { useState, useMemo } from 'react';

function AnomalyTable({ results }) {
  const [filter, setFilter] = useState('all');
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 50;

  const anomalyTypes = useMemo(() => {
    if (!results) return [];
    
    const types = [];
    const indices = new Set();
    
    // Collect all anomaly indices by type
    if (results.duplicates?.duplicate_indices) {
      results.duplicates.duplicate_indices.forEach(idx => {
        indices.add(idx);
        types.push({ index: idx, type: 'Duplicate', details: 'Duplicate claim_id' });
      });
    }
    
    if (results.missing_values?.missing_indices) {
      results.missing_values.missing_indices.forEach(idx => {
        indices.add(idx);
        types.push({ index: idx, type: 'Missing Value', details: 'Contains missing values' });
      });
    }
    
    if (results.inconsistencies?.inconsistent_indices) {
      results.inconsistencies.inconsistent_indices.forEach(idx => {
        indices.add(idx);
        const inconsistency = results.inconsistencies.inconsistencies.find(
          inc => inc.indices.includes(idx)
        );
        types.push({ 
          index: idx, 
          type: 'Inconsistency', 
          details: inconsistency?.type || 'Data inconsistency' 
        });
      });
    }
    
    if (results.statistical?.combined_anomalies) {
      // Get indices where statistical anomalies are True
      Object.keys(results.statistical.combined_anomalies).forEach(idx => {
        if (results.statistical.combined_anomalies[idx]) {
          indices.add(parseInt(idx));
          types.push({ index: parseInt(idx), type: 'Statistical Anomaly', details: 'Z-score or IQR outlier' });
        }
      });
    }
    
    if (results.ml?.combined_anomalies) {
      Object.keys(results.ml.combined_anomalies).forEach(idx => {
        if (results.ml.combined_anomalies[idx]) {
          indices.add(parseInt(idx));
          types.push({ index: parseInt(idx), type: 'ML Anomaly', details: 'Isolation Forest or LOF detected' });
        }
      });
    }
    
    return types;
  }, [results]);

  const filteredAnomalies = useMemo(() => {
    if (filter === 'all') return anomalyTypes;
    return anomalyTypes.filter(a => a.type === filter);
  }, [anomalyTypes, filter]);

  const paginatedAnomalies = useMemo(() => {
    const start = (currentPage - 1) * itemsPerPage;
    return filteredAnomalies.slice(start, start + itemsPerPage);
  }, [filteredAnomalies, currentPage]);

  const totalPages = Math.ceil(filteredAnomalies.length / itemsPerPage);

  const uniqueTypes = ['all', ...new Set(anomalyTypes.map(a => a.type))];

  if (!results || anomalyTypes.length === 0) {
    return (
      <div>
        <h2>Anomaly Details</h2>
        <p>No anomalies detected.</p>
      </div>
    );
  }

  return (
    <div>
      <h2>Anomaly Details</h2>
      
      <div style={{ marginBottom: '20px', display: 'flex', gap: '10px', alignItems: 'center' }}>
        <label style={{ fontWeight: '600' }}>Filter by Type:</label>
        <select
          className="input"
          value={filter}
          onChange={(e) => {
            setFilter(e.target.value);
            setCurrentPage(1);
          }}
          style={{ width: 'auto', minWidth: '200px' }}
        >
          {uniqueTypes.map(type => (
            <option key={type} value={type}>
              {type} {type !== 'all' && `(${anomalyTypes.filter(a => a.type === type).length})`}
            </option>
          ))}
        </select>
        <span style={{ color: '#666' }}>
          Showing {filteredAnomalies.length} of {anomalyTypes.length} anomalies
        </span>
      </div>

      <div style={{ overflowX: 'auto' }}>
        <table className="table">
          <thead>
            <tr>
              <th>Row Index</th>
              <th>Anomaly Type</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody>
            {paginatedAnomalies.map((anomaly, idx) => (
              <tr key={`${anomaly.index}-${anomaly.type}-${idx}`}>
                <td>{anomaly.index}</td>
                <td>{anomaly.type}</td>
                <td>{anomaly.details}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div style={{ marginTop: '20px', display: 'flex', gap: '10px', justifyContent: 'center' }}>
          <button
            className="button"
            onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
            disabled={currentPage === 1}
          >
            Previous
          </button>
          <span style={{ padding: '10px' }}>
            Page {currentPage} of {totalPages}
          </span>
          <button
            className="button"
            onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
            disabled={currentPage === totalPages}
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}

export default AnomalyTable;

