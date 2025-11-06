# filepath: /home/jupyter/lifeembedding/config.py
"""
LifeEmbedding Project Configuration
"""

# GCP Project Settings
PROJECT_ID = ''
PROJECT_NUMBER = ''
REGION = ''  # Primary region for services (aligned with Workbench)
WORKBENCH_ZONE = ''  # Workbench instance location

# BigQuery Settings
DATASET_ID = ''
DATASET_LOCATION = ''  # Keep in same region as Workbench for performance

# Vertex AI Settings
VERTEX_AI_REGION = ''  # Vertex AI embeddings (us-central1 has good availability)
EMBEDDING_MODEL = ''
EMBEDDING_DIMENSION = 768

# Service Account
SERVICE_ACCOUNT_EMAIL = ''

# API Settings (for later)
API_VERSION = 'v1'

# Paths
DATA_DIR = '/home/jupyter/lifeembedding/data'
LOGS_DIR = '/home/jupyter/lifeembedding/logs'

print(f"✅ Configuration loaded for project: {PROJECT_ID}")