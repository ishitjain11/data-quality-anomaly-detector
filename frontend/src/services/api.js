import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000/api';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const uploadFile = async (file) => {
  const formData = new FormData();
  formData.append('file', file);
  
  const response = await api.post('/upload', formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  });
  
  return response.data;
};

export const detectAnomalies = async (cacheKey = null) => {
  const params = cacheKey ? { cache_key: cacheKey } : {};
  const response = await api.post('/detect', null, { params });
  return response.data;
};

export const getResults = async (cacheKey = null) => {
  const params = cacheKey ? { cache_key: cacheKey } : {};
  const response = await api.get('/results', { params });
  return response.data;
};

export const generateMockData = async (numRows = 3000, errorRate = 0.15) => {
  const response = await api.post('/generate-mock-data', null, {
    params: {
      num_rows: numRows,
      error_rate: errorRate,
    },
  });
  return response.data;
};

export default api;

