import React from 'react';
import { Link } from 'react-router-dom';

const Home = () => {
  return (
    <div className="min-h-screen bg-gradient-to-b from-gray-900 via-gray-800 to-gray-900 text-white overflow-y-auto">
      <div className="container mx-auto px-4 py-16">
        {/* Hero Section */}
        <div className="text-center mb-16">
          <h1 className="text-6xl font-bold mb-6 bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
            LifeEmbedding
          </h1>
          <p className="text-2xl text-gray-300 mb-4">
            Visualize Life Trajectories in 3D Space
          </p>
          <p className="text-lg text-gray-400 max-w-3xl mx-auto mb-8">
            Explore how life events shape who we become. See how 790 notable individuals 
            cluster together based on their educational paths, career choices, and achievements.
          </p>
          <Link
            to="/explore"
            className="inline-block bg-blue-600 hover:bg-blue-700 text-white font-bold py-4 px-8 rounded-lg text-lg transition-colors"
          >
            Start Exploring →
          </Link>
        </div>

        {/* Features Grid */}
        <div className="grid md:grid-cols-3 gap-8 mb-16">
          <div className="bg-gray-800 p-6 rounded-lg">
            <div className="text-4xl mb-4">🌐</div>
            <h3 className="text-xl font-bold mb-3">3D Visualization</h3>
            <p className="text-gray-400">
              Interactive 3D scatter plot showing 790 persons across 15 distinct clusters. 
              Rotate, zoom, and explore the embedding space.
            </p>
          </div>

          <div className="bg-gray-800 p-6 rounded-lg">
            <div className="text-4xl mb-4">🎯</div>
            <h3 className="text-xl font-bold mb-3">Cluster Analysis</h3>
            <p className="text-gray-400">
              Discover meaningful patterns: baseball players, computer scientists, actors, 
              philosophers, and more cluster together naturally.
            </p>
          </div>

          <div className="bg-gray-800 p-6 rounded-lg">
            <div className="text-4xl mb-4">🔍</div>
            <h3 className="text-xl font-bold mb-3">Deep Insights</h3>
            <p className="text-gray-400">
              Click on any person to view their detailed biography, life events, 
              and see how they compare to similar individuals.
            </p>
          </div>
        </div>

        {/* Stats Section */}
        <div className="bg-gray-800 rounded-lg p-8 mb-16">
          <h2 className="text-3xl font-bold mb-6 text-center">Dataset Statistics</h2>
          <div className="grid md:grid-cols-4 gap-6 text-center">
            <div>
              <div className="text-4xl font-bold text-blue-400 mb-2">790</div>
              <div className="text-gray-400">Persons</div>
            </div>
            <div>
              <div className="text-4xl font-bold text-purple-400 mb-2">15</div>
              <div className="text-gray-400">Clusters</div>
            </div>
            <div>
              <div className="text-4xl font-bold text-green-400 mb-2">10K+</div>
              <div className="text-gray-400">Life Events</div>
            </div>
            <div>
              <div className="text-4xl font-bold text-orange-400 mb-2">768D</div>
              <div className="text-gray-400">Embeddings</div>
            </div>
          </div>
        </div>

        {/* How It Works */}
        <div className="max-w-4xl mx-auto">
          <h2 className="text-3xl font-bold mb-8 text-center">How It Works</h2>
          <div className="space-y-6">
            <div className="flex items-start">
              <div className="bg-blue-600 rounded-full w-10 h-10 flex items-center justify-center font-bold mr-4 flex-shrink-0">
                1
              </div>
              <div>
                <h3 className="text-xl font-bold mb-2">Data Collection</h3>
                <p className="text-gray-400">
                  Biographical data from Wikidata including education, employment, 
                  awards, and other significant life events.
                </p>
              </div>
            </div>

            <div className="flex items-start">
              <div className="bg-blue-600 rounded-full w-10 h-10 flex items-center justify-center font-bold mr-4 flex-shrink-0">
                2
              </div>
              <div>
                <h3 className="text-xl font-bold mb-2">Embedding Generation</h3>
                <p className="text-gray-400">
                  Life narratives are converted into 768-dimensional vectors using 
                  Google's text-embedding-004 model via Vertex AI.
                </p>
              </div>
            </div>

            <div className="flex items-start">
              <div className="bg-blue-600 rounded-full w-10 h-10 flex items-center justify-center font-bold mr-4 flex-shrink-0">
                3
              </div>
              <div>
                <h3 className="text-xl font-bold mb-2">Dimensionality Reduction</h3>
                <p className="text-gray-400">
                  PCA reduces embeddings to 50D, then UMAP projects to 3D for visualization 
                  while preserving meaningful relationships.
                </p>
              </div>
            </div>

            <div className="flex items-start">
              <div className="bg-blue-600 rounded-full w-10 h-10 flex items-center justify-center font-bold mr-4 flex-shrink-0">
                4
              </div>
              <div>
                <h3 className="text-xl font-bold mb-2">Clustering & Visualization</h3>
                <p className="text-gray-400">
                  K-means clustering identifies 15 distinct groups, visualized in an 
                  interactive 3D space you can explore.
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* CTA */}
        <div className="text-center mt-16">
          <Link
            to="/explore"
            className="inline-block bg-purple-600 hover:bg-purple-700 text-white font-bold py-4 px-8 rounded-lg text-lg transition-colors"
          >
            Explore the Life Embedding Space →
          </Link>
        </div>
      </div>
    </div>
  );
};

export default Home;
