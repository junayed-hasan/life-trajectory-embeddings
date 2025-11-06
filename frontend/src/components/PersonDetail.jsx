import React, { useState, useEffect } from 'react';
import { apiService } from '../services/api';

const PersonDetail = ({ personId, onClose }) => {
  const [person, setPerson] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!personId) return;

    const fetchPersonDetail = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await apiService.getPersonById(personId);
        setPerson(data);
      } catch (err) {
        setError('Failed to load person details');
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    fetchPersonDetail();
  }, [personId]);

  if (!personId) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-900 text-white rounded-lg shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="sticky top-0 bg-gray-800 p-4 border-b border-gray-700 flex justify-between items-center">
          <h2 className="text-xl font-bold">Person Details</h2>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-white text-2xl font-bold px-3"
          >
            ×
          </button>
        </div>

        {/* Content */}
        <div className="p-6">
          {loading && (
            <div className="flex justify-center items-center py-12">
              <div className="spinner"></div>
            </div>
          )}

          {error && (
            <div className="bg-red-900 bg-opacity-50 text-red-200 p-4 rounded-lg">
              {error}
            </div>
          )}

          {person && !loading && (
            <div className="space-y-6">
              {/* Basic Info */}
              <div>
                <h3 className="text-2xl font-bold mb-2">{person.name}</h3>
                {person.description && (
                  <p className="text-gray-300 mb-4">{person.description}</p>
                )}
              </div>

              {/* Biographical Info */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {person.birth_date && (
                  <div className="bg-gray-800 p-3 rounded-lg">
                    <div className="text-gray-400 text-sm">Born</div>
                    <div className="font-semibold">
                      {person.birth_date}
                      {person.birth_place && ` in ${person.birth_place}`}
                    </div>
                  </div>
                )}

                {person.death_date && (
                  <div className="bg-gray-800 p-3 rounded-lg">
                    <div className="text-gray-400 text-sm">Died</div>
                    <div className="font-semibold">
                      {person.death_date}
                      {person.death_place && ` in ${person.death_place}`}
                    </div>
                  </div>
                )}
              </div>

              {/* Occupations */}
              {person.occupation && person.occupation.length > 0 && (
                <div className="bg-gray-800 p-4 rounded-lg">
                  <div className="text-gray-400 text-sm mb-2">Occupations</div>
                  <div className="flex flex-wrap gap-2">
                    {person.occupation.map((occ, idx) => (
                      <span
                        key={idx}
                        className="bg-blue-600 px-3 py-1 rounded-full text-sm"
                      >
                        {occ}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {/* Cluster Info */}
              {person.cluster_id !== null && person.cluster_id !== undefined && (
                <div className="bg-gray-800 p-4 rounded-lg">
                  <div className="text-gray-400 text-sm mb-2">Cluster</div>
                  <div className="font-semibold">
                    {person.cluster_label || `Cluster ${person.cluster_id}`}
                  </div>
                  {person.coordinates && (
                    <div className="text-gray-400 text-xs mt-2">
                      Position: ({person.coordinates.x.toFixed(2)}, {person.coordinates.y.toFixed(2)}, {person.coordinates.z.toFixed(2)})
                    </div>
                  )}
                </div>
              )}

              {/* Life Events Summary */}
              {person.life_events_count > 0 && (
                <div className="bg-gray-800 p-4 rounded-lg">
                  <div className="text-gray-400 text-sm mb-3">
                    Life Events ({person.life_events_count} total)
                  </div>
                  {person.event_types && Object.keys(person.event_types).length > 0 && (
                    <div className="space-y-2">
                      {Object.entries(person.event_types).map(([type, count]) => (
                        <div key={type} className="flex justify-between items-center">
                          <span className="text-sm capitalize">
                            {type.replace('_', ' ')}
                          </span>
                          <span className="bg-gray-700 px-2 py-1 rounded text-xs">
                            {count}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {/* Wikidata Link */}
              {person.wikidata_id && (
                <div className="pt-4 border-t border-gray-700">
                  <a
                    href={`https://www.wikidata.org/wiki/${person.wikidata_id}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-blue-400 hover:text-blue-300 text-sm"
                  >
                    View on Wikidata →
                  </a>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default PersonDetail;
