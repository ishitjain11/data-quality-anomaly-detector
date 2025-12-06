import React from 'react';
import {
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d'];

function Visualizations({ results }) {
  if (!results) {
    return <div>No data to visualize</div>;
  }

  // Prepare data for anomaly type distribution
  const anomalyTypeData = [
    {
      name: 'Duplicates',
      value: results.summary?.duplicate_count || 0
    },
    {
      name: 'Missing Values',
      value: results.summary?.missing_value_count || 0
    },
    {
      name: 'Inconsistencies',
      value: results.summary?.inconsistency_count || 0
    },
    {
      name: 'Statistical',
      value: results.summary?.statistical_anomaly_count || 0
    },
    {
      name: 'ML Anomalies',
      value: results.summary?.ml_anomaly_count || 0
    }
  ].filter(item => item.value > 0);

  // Prepare data for missing values by column
  const missingValuesData = results.missing_values?.missing_counts
    ? Object.entries(results.missing_values.missing_counts)
        .filter(([_, count]) => count > 0)
        .map(([column, count]) => ({
          name: column,
          value: count
        }))
    : [];

  // Prepare data for inconsistency types
  const inconsistencyData = results.inconsistencies?.inconsistencies
    ? results.inconsistencies.inconsistencies.map(inc => ({
        name: inc.type.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
        value: inc.count
      }))
    : [];

  return (
    <div>
      <h2>Visualizations</h2>
      
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))', 
        gap: '30px',
        marginTop: '20px'
      }}>
        {/* Anomaly Type Distribution - Pie Chart */}
        {anomalyTypeData.length > 0 && (
          <div>
            <h3 style={{ marginBottom: '15px', fontSize: '18px' }}>Anomaly Type Distribution</h3>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={anomalyTypeData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {anomalyTypeData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Missing Values by Column - Bar Chart */}
        {missingValuesData.length > 0 && (
          <div>
            <h3 style={{ marginBottom: '15px', fontSize: '18px' }}>Missing Values by Column</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={missingValuesData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} />
                <YAxis />
                <Tooltip />
                <Bar dataKey="value" fill="#8884d8" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Inconsistency Types - Bar Chart */}
        {inconsistencyData.length > 0 && (
          <div>
            <h3 style={{ marginBottom: '15px', fontSize: '18px' }}>Inconsistency Types</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={inconsistencyData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} />
                <YAxis />
                <Tooltip />
                <Bar dataKey="value" fill="#82ca9d" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Summary Statistics */}
        <div>
          <h3 style={{ marginBottom: '15px', fontSize: '18px' }}>Summary Statistics</h3>
          <div style={{ padding: '20px', background: '#f8f9fa', borderRadius: '8px' }}>
            <div style={{ marginBottom: '10px' }}>
              <strong>Total Rows:</strong> {results.summary?.total_rows || 0}
            </div>
            <div style={{ marginBottom: '10px' }}>
              <strong>Total Anomalies:</strong> {results.summary?.total_anomalies || 0}
            </div>
            <div style={{ marginBottom: '10px' }}>
              <strong>Anomaly Rate:</strong>{' '}
              {((results.summary?.anomaly_rate || 0) * 100).toFixed(2)}%
            </div>
            <div style={{ marginBottom: '10px' }}>
              <strong>Duplicates:</strong> {results.summary?.duplicate_count || 0}
            </div>
            <div style={{ marginBottom: '10px' }}>
              <strong>Missing Values:</strong> {results.summary?.missing_value_count || 0}
            </div>
            <div style={{ marginBottom: '10px' }}>
              <strong>Inconsistencies:</strong> {results.summary?.inconsistency_count || 0}
            </div>
            <div style={{ marginBottom: '10px' }}>
              <strong>Statistical Anomalies:</strong> {results.summary?.statistical_anomaly_count || 0}
            </div>
            <div>
              <strong>ML Anomalies:</strong> {results.summary?.ml_anomaly_count || 0}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Visualizations;

