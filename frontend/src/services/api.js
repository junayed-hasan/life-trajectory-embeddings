import axios from 'axios';

// API Base URL - Use proxy path for Workbench or direct localhost
// When accessing through Workbench proxy, backend is also proxied
const API_BASE_URL = window.location.hostname === 'localhost' 
  ? 'http://localhost:8081/api/v1'
  : '/proxy/8081/api/v1';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// API Service Functions
export const apiService = {
  // Get all persons (paginated)
  getPersons: async (limit = 100, offset = 0, clusterId = null) => {
    try {
      const params = { limit, offset };
      if (clusterId !== null) {
        params.cluster_id = clusterId;
      }
      const response = await api.get('/persons', { params });
      return response.data;
    } catch (error) {
      console.error('Error fetching persons:', error);
      throw error;
    }
  },

  // Get visualization data (all persons with 3D coordinates)
  getVisualizationData: async () => {
    try {
      const response = await api.get('/visualization');
      return response.data;
    } catch (error) {
      console.error('Error fetching visualization data:', error);
      throw error;
    }
  },

  // Get person details by ID
  getPersonById: async (personId) => {
    try {
      const response = await api.get(`/person/${personId}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching person details:', error);
      throw error;
    }
  },

  // Get all clusters
  getClusters: async () => {
    try {
      const response = await api.get('/clusters');
      return response.data;
    } catch (error) {
      console.error('Error fetching clusters:', error);
      throw error;
    }
  },

  // Get specific cluster
  getClusterById: async (clusterId) => {
    try {
      const response = await api.get(`/cluster/${clusterId}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching cluster:', error);
      throw error;
    }
  },

  // Health check
  healthCheck: async () => {
    try {
      const baseURL = window.location.hostname === 'localhost'
        ? 'http://localhost:8081/api'
        : '/proxy/8081/api';
      const response = await api.get('/health', {
        baseURL: baseURL,
      });
      return response.data;
    } catch (error) {
      console.error('Error checking health:', error);
      throw error;
    }
  },

  // Generate user embedding (optional - may not work if models not loaded)
  generateEmbedding: async (lifeEvents) => {
    try {
      const response = await api.post('/generate-embedding', {
        life_events: lifeEvents,
      });
      return response.data;
    } catch (error) {
      console.error('Error generating embedding:', error);
      throw error;
    }
  },
};

export default api;
