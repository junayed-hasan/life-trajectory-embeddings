import React from 'react';
import { getClusterColor, DEFAULT_CLUSTER_LABELS } from '../utils/colors';

const ClusterLegend = ({ clusters, selectedCluster, onClusterSelect }) => {
  // Use clusters from API or fall back to defaults
  const clusterList = clusters.length > 0
    ? clusters.map(c => ({
        id: c.cluster_id,
        label: c.cluster_label || DEFAULT_CLUSTER_LABELS[c.cluster_id] || `Cluster ${c.cluster_id}`,
        count: c.person_count,
      }))
    : Object.entries(DEFAULT_CLUSTER_LABELS).map(([id, label]) => ({
        id: parseInt(id),
        label,
        count: 0,
      }));

  return (
    <div className="bg-gray-900 text-white p-4 rounded-lg shadow-lg overflow-y-auto max-h-full">
      <h3 className="text-lg font-bold mb-4 sticky top-0 bg-gray-900 pb-2">
        Cluster Legend
      </h3>
      <div className="space-y-2">
        {/* Show All option */}
        <div
          className={`cursor-pointer p-3 rounded-lg transition-all ${
            selectedCluster === null
              ? 'bg-blue-600 ring-2 ring-blue-400'
              : 'bg-gray-800 hover:bg-gray-700'
          }`}
          onClick={() => onClusterSelect(null)}
        >
          <div className="flex items-center">
            <div
              className="w-4 h-4 rounded-full mr-3"
              style={{
                background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
              }}
            />
            <div className="flex-1">
              <div className="font-semibold">All Clusters</div>
              <div className="text-xs text-gray-400">Show all persons</div>
            </div>
          </div>
        </div>

        {/* Individual clusters */}
        {clusterList.map((cluster) => {
          const color = getClusterColor(cluster.id);
          return (
            <div
              key={cluster.id}
              className={`cursor-pointer p-3 rounded-lg transition-all ${
                selectedCluster === cluster.id
                  ? 'bg-blue-600 ring-2 ring-blue-400'
                  : 'bg-gray-800 hover:bg-gray-700'
              }`}
              onClick={() => onClusterSelect(cluster.id)}
            >
              <div className="flex items-center">
                <div
                  className="w-4 h-4 rounded-full mr-3"
                  style={{
                    backgroundColor: `rgb(${color[0]}, ${color[1]}, ${color[2]})`,
                  }}
                />
                <div className="flex-1">
                  <div className="font-semibold text-sm">{cluster.label}</div>
                  {cluster.count > 0 && (
                    <div className="text-xs text-gray-400">{cluster.count} persons</div>
                  )}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default ClusterLegend;
