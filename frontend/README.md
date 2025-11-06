# LifeEmbedding Frontend

React-based frontend for visualizing life trajectories in 3D space.

## Setup

### Prerequisites

- Node.js 18+ and npm
- Backend API running on port 8081

### Installation

1. Install dependencies:

```bash
cd frontend
npm install
```

2. Start development server:

```bash
npm run dev
```

The app will be available at `http://localhost:3000`

## Project Structure

```
frontend/
├── src/
│   ├── components/
│   │   ├── Navigation.jsx       # Top navigation bar
│   │   ├── VisualizationView.jsx # 3D deck.gl visualization
│   │   ├── ClusterLegend.jsx    # Cluster filter sidebar
│   │   └── PersonDetail.jsx     # Person detail modal
│   ├── pages/
│   │   ├── Home.jsx             # Landing page
│   │   ├── Explore.jsx          # Main visualization page
│   │   └── About.jsx            # About page
│   ├── services/
│   │   └── api.js               # API service layer
│   ├── utils/
│   │   └── colors.js            # Cluster color mappings
│   ├── App.jsx                  # Main app component
│   ├── main.jsx                 # Entry point
│   └── index.css                # Global styles
├── index.html
├── package.json
├── vite.config.js
└── tailwind.config.js
```

## Features

### 3D Visualization

- Interactive deck.gl ScatterplotLayer with 790 persons
- Color-coded by 15 clusters
- Hover tooltips with person info
- Click to view detailed biography
- Orbit controls (rotate, zoom, pan)

### Cluster Filtering

- Sidebar legend showing all 15 clusters
- Click to filter by specific cluster
- Real-time updates to visualization

### Person Details

- Modal showing full biography
- Life events breakdown by type
- Birth/death dates and places
- Occupations and cluster assignment
- Link to Wikidata source

### Pages

- **Home**: Landing page with project overview
- **Explore**: Main 3D visualization interface
- **About**: Technical details and research findings

## API Configuration

The frontend connects to the backend API at `http://localhost:8081/api/v1`.

To change the API URL, edit `src/services/api.js`:

```javascript
const API_BASE_URL = "http://localhost:8081/api/v1";
```

## Building for Production

```bash
npm run build
```

This creates an optimized production build in the `dist/` directory.

Preview the production build:

```bash
npm run preview
```

## Technology Stack

- **React 18**: UI framework
- **Vite**: Build tool and dev server
- **deck.gl**: 3D visualization library
- **Tailwind CSS**: Utility-first CSS framework
- **Axios**: HTTP client for API calls
- **React Router**: Client-side routing

## Troubleshooting

### Backend Connection Issues

- Ensure backend is running: `uvicorn main:app --reload --host 0.0.0.0 --port 8081`
- Check CORS is enabled in backend (should allow all origins in dev)
- Verify API URL in `src/services/api.js` matches your backend port

### 3D Visualization Not Loading

- Check browser console for errors
- Ensure visualization endpoint returns data: `curl http://localhost:8081/api/v1/visualization`
- Verify persons have valid x, y, z coordinates

### Performance Issues

- Reduce `radiusMinPixels` in VisualizationView.jsx
- Limit number of persons rendered (add pagination)
- Disable stroked outlines on points

## Next Steps

1. Test all pages and interactions
2. Verify cluster filtering works correctly
3. Test person detail modal with various persons
4. Check responsive design on different screen sizes
5. Build and preview production version
6. Deploy to Cloud Run (Phase 8)

## Notes

- The user embedding endpoint (POST /generate-embedding) is not implemented in the UI yet since the backend models aren't loaded
- You can add this feature later by creating a UserInputForm component
- All 790 persons should be visible in the 3D space
- Performance is good for 790 points with deck.gl's WebGL rendering
