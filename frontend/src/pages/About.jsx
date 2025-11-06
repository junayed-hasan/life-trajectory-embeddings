import React from 'react';

const About = () => {
  return (
    <div className="min-h-screen bg-gray-900 text-white overflow-y-auto">
      <div className="container mx-auto px-4 py-12 max-w-4xl">
        <h1 className="text-4xl font-bold mb-8">About LifeEmbedding</h1>

        {/* Project Overview */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold mb-4">Project Overview</h2>
          <p className="text-gray-300 mb-4">
            LifeEmbedding is a cloud-based system that visualizes the trajectories of 
            notable individuals in a 3D embedding space. By analyzing biographical data 
            and life events, we can understand how different career paths, educational 
            backgrounds, and achievements cluster together.
          </p>
          <p className="text-gray-300">
            This project was developed for JHU's Cloud Computing course (EN.605.788) 
            to demonstrate practical applications of embeddings, dimensionality reduction, 
            and cloud infrastructure.
          </p>
        </section>

        {/* Research Questions */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold mb-4">Research Questions</h2>
          <div className="space-y-4">
            <div className="bg-gray-800 p-4 rounded-lg">
              <h3 className="font-bold text-lg mb-2">
                1. How can we represent life trajectories as embeddings?
              </h3>
              <p className="text-gray-400">
                We convert biographical narratives into 768-dimensional embeddings using 
                Google's text-embedding-004 model, capturing semantic relationships between 
                life events.
              </p>
            </div>

            <div className="bg-gray-800 p-4 rounded-lg">
              <h3 className="font-bold text-lg mb-2">
                2. Do individuals with similar roles cluster together?
              </h3>
              <p className="text-gray-400">
                Yes! Our K-means clustering with k=15 reveals distinct occupational groups: 
                baseball players (100% pure), badminton players (96%), computer scientists, 
                actors, philosophers, and more.
              </p>
            </div>

            <div className="bg-gray-800 p-4 rounded-lg">
              <h3 className="font-bold text-lg mb-2">
                3. How do life events shape trajectories?
              </h3>
              <p className="text-gray-400">
                By analyzing the 3D coordinates, we can see how education, career choices, 
                and achievements influence one's position in the embedding space.
              </p>
            </div>
          </div>
        </section>

        {/* Technical Architecture */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold mb-4">Technical Architecture</h2>
          <div className="bg-gray-800 p-6 rounded-lg">
            <div className="space-y-4">
              <div>
                <h3 className="font-bold mb-2">Data Pipeline</h3>
                <ul className="list-disc list-inside text-gray-400 space-y-1">
                  <li>Wikidata SPARQL queries for biographical data</li>
                  <li>BigQuery for data storage and querying</li>
                  <li>790 persons with 10,000+ life events</li>
                </ul>
              </div>

              <div>
                <h3 className="font-bold mb-2">Embedding Generation</h3>
                <ul className="list-disc list-inside text-gray-400 space-y-1">
                  <li>Vertex AI text-embedding-004 model</li>
                  <li>768-dimensional embeddings</li>
                  <li>Event-based narrative generation</li>
                </ul>
              </div>

              <div>
                <h3 className="font-bold mb-2">Dimensionality Reduction</h3>
                <ul className="list-disc list-inside text-gray-400 space-y-1">
                  <li>PCA: 768D → 50D (53.36% variance preserved)</li>
                  <li>UMAP: 50D → 3D (cosine metric, n_neighbors=15)</li>
                  <li>K-means clustering with k=15</li>
                </ul>
              </div>

              <div>
                <h3 className="font-bold mb-2">Backend API</h3>
                <ul className="list-disc list-inside text-gray-400 space-y-1">
                  <li>FastAPI with 7 REST endpoints</li>
                  <li>BigQuery integration for data access</li>
                  <li>Pydantic validation for data models</li>
                </ul>
              </div>

              <div>
                <h3 className="font-bold mb-2">Frontend</h3>
                <ul className="list-disc list-inside text-gray-400 space-y-1">
                  <li>React with Vite for fast development</li>
                  <li>deck.gl for 3D visualization</li>
                  <li>Tailwind CSS for styling</li>
                </ul>
              </div>
            </div>
          </div>
        </section>

        {/* Key Findings */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold mb-4">Key Findings</h2>
          <div className="space-y-3">
            <div className="bg-green-900 bg-opacity-30 p-4 rounded-lg border-l-4 border-green-500">
              <p className="text-gray-300">
                <strong>Sports clusters are highly pure:</strong> Baseball (100%) and 
                badminton (96%) players cluster almost perfectly, showing strong 
                occupational identity.
              </p>
            </div>

            <div className="bg-blue-900 bg-opacity-30 p-4 rounded-lg border-l-4 border-blue-500">
              <p className="text-gray-300">
                <strong>Academic/intellectual roles overlap:</strong> Writers, philosophers, 
                and academics show more mixing, reflecting interdisciplinary careers.
              </p>
            </div>

            <div className="bg-purple-900 bg-opacity-30 p-4 rounded-lg border-l-4 border-purple-500">
              <p className="text-gray-300">
                <strong>STEM has its own cluster:</strong> Computer scientists and 
                mathematicians form a distinct group (Cluster 13).
              </p>
            </div>

            <div className="bg-orange-900 bg-opacity-30 p-4 rounded-lg border-l-4 border-orange-500">
              <p className="text-gray-300">
                <strong>Clustering quality is good:</strong> Silhouette score of 0.42 
                indicates meaningful, well-separated clusters.
              </p>
            </div>
          </div>
        </section>

        {/* Technology Stack */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold mb-4">Technology Stack</h2>
          <div className="grid md:grid-cols-2 gap-4">
            <div className="bg-gray-800 p-4 rounded-lg">
              <h3 className="font-bold mb-3">Cloud Infrastructure</h3>
              <ul className="text-gray-400 space-y-1">
                <li>• Google Cloud Platform</li>
                <li>• Vertex AI Workbench</li>
                <li>• BigQuery</li>
                <li>• Vertex AI (Embeddings)</li>
              </ul>
            </div>

            <div className="bg-gray-800 p-4 rounded-lg">
              <h3 className="font-bold mb-3">Backend</h3>
              <ul className="text-gray-400 space-y-1">
                <li>• Python 3.10+</li>
                <li>• FastAPI</li>
                <li>• Uvicorn</li>
                <li>• Pydantic</li>
              </ul>
            </div>

            <div className="bg-gray-800 p-4 rounded-lg">
              <h3 className="font-bold mb-3">Frontend</h3>
              <ul className="text-gray-400 space-y-1">
                <li>• React 18</li>
                <li>• Vite</li>
                <li>• deck.gl</li>
                <li>• Tailwind CSS</li>
              </ul>
            </div>

            <div className="bg-gray-800 p-4 rounded-lg">
              <h3 className="font-bold mb-3">ML/Data Science</h3>
              <ul className="text-gray-400 space-y-1">
                <li>• scikit-learn (PCA, K-means)</li>
                <li>• UMAP</li>
                <li>• NumPy</li>
                <li>• text-embedding-004</li>
              </ul>
            </div>
          </div>
        </section>

        {/* Future Work */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold mb-4">Future Enhancements</h2>
          <ul className="list-disc list-inside text-gray-400 space-y-2">
            <li>User input for custom life trajectories</li>
            <li>Counterfactual analysis ("what if" scenarios)</li>
            <li>Temporal trajectory animation showing life progression</li>
            <li>Hierarchical clustering for multi-level exploration</li>
            <li>Expanded dataset (1000+ persons, contemporary figures)</li>
            <li>Cloud Run deployment for public access</li>
          </ul>
        </section>

        {/* Contact */}
        <section>
          <h2 className="text-2xl font-bold mb-4">Project Information</h2>
          <div className="bg-gray-800 p-4 rounded-lg text-gray-400">
            <p className="mb-2">
              <strong className="text-white">Course:</strong> EN.605.788 Cloud Computing
            </p>
            <p className="mb-2">
              <strong className="text-white">Institution:</strong> Johns Hopkins University
            </p>
            <p>
              <strong className="text-white">Repository:</strong>{' '}
              <a
                href="https://github.com/your-username/lifeembedding"
                className="text-blue-400 hover:text-blue-300"
              >
                GitHub (Update with your repo)
              </a>
            </p>
          </div>
        </section>
      </div>
    </div>
  );
};

export default About;
