import React, { useState, useEffect } from 'react';
import { apiService } from '../services/api';
import VisualizationView from '../components/VisualizationView';
import ClusterLegend from '../components/ClusterLegend';
import PersonDetail from '../components/PersonDetail';

const Explore = () => {
  const [persons, setPersons] = useState([]);
  const [allPersons, setAllPersons] = useState([]);
  const [clusters, setClusters] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedCluster, setSelectedCluster] = useState(null);
  const [selectedPersonId, setSelectedPersonId] = useState(null);

  // Load initial data
  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      setError(null);
      try {
        // Load visualization data and clusters in parallel
        const [vizData, clustersData] = await Promise.all([
          apiService.getVisualizationData(),
          apiService.getClusters(),
        ]);

        setAllPersons(vizData.persons || []);
        setPersons(vizData.persons || []);
        setClusters(clustersData.clusters || []);
      } catch (err) {
        setError('Failed to load data. Make sure the backend is running on port 8081.');
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, []);

  // Filter persons by cluster
  useEffect(() => {
    if (selectedCluster === null) {
      setPersons(allPersons);
    } else {
      setPersons(allPersons.filter((p) => p.cluster_id === selectedCluster));
    }
  }, [selectedCluster, allPersons]);

  const handleClusterSelect = (clusterId) => {
    setSelectedCluster(clusterId);
    setSelectedPersonId(null); // Clear selection when changing cluster
  };

  const handlePersonClick = (person) => {
    setSelectedPersonId(person.person_id);
  };

  const handleCloseDetail = () => {
    setSelectedPersonId(null);
  };

  if (loading) {
    return (
      <div className="h-screen flex items-center justify-center bg-gray-900">
        <div className="text-center">
          <div className="spinner mx-auto mb-4"></div>
          <div className="text-white text-lg">Loading visualization data...</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="h-screen flex items-center justify-center bg-gray-900">
        <div className="bg-red-900 bg-opacity-50 text-red-200 p-6 rounded-lg max-w-md">
          <div className="font-bold text-xl mb-2">Error</div>
          <div>{error}</div>
          <button
            onClick={() => window.location.reload()}
            className="mt-4 bg-red-700 hover:bg-red-600 px-4 py-2 rounded"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="h-[calc(100vh-4rem)] flex flex-col bg-gray-900">
      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Sidebar - Cluster Legend */}
        <div className="w-80 bg-gray-800 border-r border-gray-700 overflow-y-auto">
          <ClusterLegend
            clusters={clusters}
            selectedCluster={selectedCluster}
            onClusterSelect={handleClusterSelect}
          />
        </div>

        {/* Visualization */}
        <div className="flex-1 relative">
          <VisualizationView
            persons={persons}
            clusters={clusters}
            onPersonClick={handlePersonClick}
            selectedPersonId={selectedPersonId}
            selectedCluster={selectedCluster}
          />
        </div>
      </div>

      {/* Person Detail Modal */}
      {selectedPersonId && (
        <PersonDetail personId={selectedPersonId} onClose={handleCloseDetail} />
      )}
    </div>
  );
};

export default Explore;
