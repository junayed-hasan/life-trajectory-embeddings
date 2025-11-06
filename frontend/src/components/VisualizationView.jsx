import React, { useState, useEffect } from 'react';
import DeckGL from '@deck.gl/react';
import { ScatterplotLayer, LineLayer } from '@deck.gl/layers';
import { OrbitView } from '@deck.gl/core';
import { getClusterColor } from '../utils/colors';

const INITIAL_VIEW_STATE = {
  target: [0, 7, 11],
  rotationX: 60,
  rotationOrbit: 45,
  zoom: -1,
  minZoom: -5,
  maxZoom: 5,
};

const VisualizationView = ({ persons, clusters, onPersonClick, selectedPersonId, selectedCluster }) => {
  const [viewState, setViewState] = useState(INITIAL_VIEW_STATE);
  const [hoveredPerson, setHoveredPerson] = useState(null);

  // Calculate number of unique clusters in current persons list
  const uniqueClusters = [...new Set(persons.map(p => p.cluster_id).filter(c => c !== null && c !== undefined))];
  const clusterCount = uniqueClusters.length;

  // Generate line data from origin to each person
  const lineData = persons.map(person => ({
    source: [0, 7, 11], // Origin point (center of data)
    target: [person.x, person.y, person.z],
    person: person,
  }));

  // Create line layer for trajectories
  const lineLayer = new LineLayer({
    id: 'trajectories',
    data: lineData,
    pickable: false,
    getSourcePosition: d => d.source,
    getTargetPosition: d => d.target,
    getColor: d => {
      const color = getClusterColor(d.person.cluster_id);
      return [...color, 60]; // More transparent lines
    },
    getWidth: 1,
    widthMinPixels: 0.5,
    widthMaxPixels: 2,
  });

  // Create scatterplot layer for persons
  const scatterLayer = new ScatterplotLayer({
    id: 'persons-scatterplot',
    data: persons,
    pickable: true,
    opacity: 0.95,
    stroked: true,
    filled: true,
    radiusScale: 0.15,
    radiusMinPixels: 6,
    radiusMaxPixels: 20,
    lineWidthMinPixels: 1.5,
    getPosition: (d) => [d.x, d.y, d.z],
    getRadius: (d) => (d.person_id === selectedPersonId ? 0.25 : 0.12),
    getFillColor: (d) => {
      if (d.person_id === selectedPersonId) {
        return [255, 255, 0, 255]; // Bright yellow for selected
      }
      const color = getClusterColor(d.cluster_id);
      return [...color, 255]; // Fully opaque
    },
    getLineColor: (d) => {
      if (d.person_id === selectedPersonId) {
        return [255, 255, 255, 255]; // White border for selected
      }
      return [0, 0, 0, 180]; // Dark border for contrast
    },
    onHover: (info) => {
      if (info.object) {
        setHoveredPerson(info.object);
      } else {
        setHoveredPerson(null);
      }
    },
    onClick: (info) => {
      if (info.object && onPersonClick) {
        onPersonClick(info.object);
      }
    },
    updateTriggers: {
      getRadius: [selectedPersonId],
      getFillColor: [selectedPersonId],
      getLineColor: [selectedPersonId],
    },
  });

  return (
    <div className="relative w-full h-full bg-gray-950">
      <DeckGL
        views={new OrbitView()}
        viewState={viewState}
        onViewStateChange={({ viewState }) => setViewState(viewState)}
        controller={true}
        layers={[lineLayer, scatterLayer]}
        getCursor={() => (hoveredPerson ? 'pointer' : 'grab')}
        style={{ background: '#030712' }}
      >
        {/* Tooltip */}
        {hoveredPerson && (
          <div
            style={{
              position: 'absolute',
              zIndex: 1,
              pointerEvents: 'none',
              left: '50%',
              top: 20,
              transform: 'translateX(-50%)',
              background: 'rgba(0, 0, 0, 0.95)',
              color: 'white',
              padding: '14px 18px',
              borderRadius: '10px',
              fontSize: '14px',
              maxWidth: '350px',
              boxShadow: '0 8px 16px rgba(0, 0, 0, 0.5)',
              border: '1px solid rgba(255, 255, 255, 0.1)',
            }}
          >
            <div className="font-bold text-xl mb-2">{hoveredPerson.name}</div>
            <div className="text-gray-300 text-sm mb-2">
              {hoveredPerson.occupation?.join(', ') || 'Unknown occupation'}
            </div>
            {hoveredPerson.cluster_label && (
              <div 
                className="text-xs mt-2 inline-block px-2 py-1 rounded"
                style={{ 
                  backgroundColor: `rgba(${getClusterColor(hoveredPerson.cluster_id).join(',')}, 0.3)`,
                  border: `1px solid rgb(${getClusterColor(hoveredPerson.cluster_id).join(',')})`,
                }}
              >
                {hoveredPerson.cluster_label}
              </div>
            )}
          </div>
        )}
      </DeckGL>

      {/* Controls Info */}
      <div className="absolute bottom-4 left-4 bg-black bg-opacity-90 text-white p-4 rounded-lg text-sm border border-gray-700 shadow-xl">
        <div className="font-bold mb-3 text-blue-400">🎮 Controls</div>
        <div className="space-y-1 text-xs">
          <div>🖱️ <span className="text-gray-400">Left Drag:</span> Rotate</div>
          <div>🖱️ <span className="text-gray-400">Right Drag:</span> Pan</div>
          <div>🖱️ <span className="text-gray-400">Scroll:</span> Zoom</div>
          <div>🖱️ <span className="text-gray-400">Click Point:</span> Details</div>
        </div>
      </div>

      {/* Stats */}
      <div className="absolute top-4 left-4 bg-black bg-opacity-90 text-white p-5 rounded-lg border border-gray-700 shadow-xl">
        <div className="font-bold text-xl mb-3 bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
          Life Embedding Space
        </div>
        <div className="space-y-2 text-sm">
          <div className="flex items-center justify-between">
            <span className="text-gray-400">Persons:</span>
            <span className="font-bold text-blue-300">{persons.length}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-gray-400">Clusters:</span>
            <span className="font-bold text-purple-300">{clusterCount}</span>
          </div>
          {selectedCluster !== null && (
            <div className="text-xs text-gray-500 mt-2 pt-2 border-t border-gray-700">
              Filtered to cluster {selectedCluster}
            </div>
          )}
        </div>
      </div>

      {/* Legend hint */}
      <div className="absolute top-4 right-4 bg-black bg-opacity-90 text-white p-4 rounded-lg border border-gray-700 shadow-xl text-xs">
        <div className="font-bold mb-2 text-green-400">💡 Visualization</div>
        <div className="text-gray-400 space-y-1">
          <div>• Lines show trajectories from origin</div>
          <div>• Colors represent different clusters</div>
          <div>• Larger spheres = selected person</div>
        </div>
      </div>
    </div>
  );
};

export default VisualizationView;
