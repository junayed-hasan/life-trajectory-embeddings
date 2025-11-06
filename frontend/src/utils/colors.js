// Color palette for 15 clusters - Vibrant and distinct
export const CLUSTER_COLORS = [
  [255, 59, 48],    // Cluster 0 - Bright Red (Sports - Football/Basketball)
  [52, 199, 89],    // Cluster 1 - Green (Academic Writers)
  [138, 43, 226],   // Cluster 2 - Blue Violet (Teachers/Judges)
  [255, 149, 0],    // Cluster 3 - Orange (Political Screenwriters)
  [0, 122, 255],    // Cluster 4 - Blue (Writers/Journalists)
  [255, 45, 85],    // Cluster 5 - Pink (Politicians)
  [175, 82, 222],   // Cluster 6 - Purple (Academic/TV)
  [255, 204, 0],    // Cluster 7 - Yellow (Philosophers)
  [10, 189, 227],   // Cluster 8 - Cyan (STEM Academics)
  [255, 55, 95],    // Cluster 9 - Hot Pink (Baseball)
  [88, 86, 214],    // Cluster 10 - Indigo (Theologians)
  [255, 159, 10],   // Cluster 11 - Amber (Actors/Entertainers)
  [48, 209, 88],    // Cluster 12 - Lime (Badminton)
  [90, 200, 250],   // Cluster 13 - Sky Blue (Computer Scientists)
  [255, 115, 50],   // Cluster 14 - Coral (Artist-Philosophers)
];

// Get color for a specific cluster
export const getClusterColor = (clusterId) => {
  if (clusterId === null || clusterId === undefined) {
    return [200, 200, 200]; // Gray for no cluster
  }
  return CLUSTER_COLORS[clusterId % CLUSTER_COLORS.length];
};

// Cluster labels mapping (will be updated from API)
export const DEFAULT_CLUSTER_LABELS = {
  0: 'Sports - Football/Basketball',
  1: 'Academic Writers',
  2: 'Teachers/Judges',
  3: 'Political Screenwriters',
  4: 'Writers/Journalists',
  5: 'Politicians',
  6: 'Academic/TV Presenters',
  7: 'Philosophers',
  8: 'STEM Academics',
  9: 'Baseball Players',
  10: 'Theologians',
  11: 'Actors/Entertainers',
  12: 'Badminton Players',
  13: 'Computer Scientists',
  14: 'Artist-Philosophers',
};
