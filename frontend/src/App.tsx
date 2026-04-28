import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './Layout';
import Overview from './pages/Overview';
import Stub from './pages/Stub';

// Mounted under /app/ in production (see vite.config.ts `base`).
// During dev (vite serve) the basename is '/app' too — the dev server
// rewrites unknown paths back to index.html.
export default function App() {
  return (
    <BrowserRouter basename="/app">
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Overview />} />
          <Route
            path="revenue"
            element={
              <Stub
                name="Revenue"
                hint="Fetch /static/data/revenue-{window}.json and render monthly + by_zip with react-chartjs-2 (Line + Bar)."
              />
            }
          />
          <Route
            path="inspections"
            element={
              <Stub
                name="Inspections"
                hint="Fetch /static/data/inspections-{window}.json. Doughnut for distribution, sortable table for repeat_offenders."
              />
            }
          />
          <Route
            path="correlation"
            element={
              <Stub
                name="Correlation & Lifecycle"
                hint="Two scatters (score↔revenue with confidence slider, tenure↔score) + status doughnut + churn table from /static/data/correlation-{window}.json and /static/data/lifecycle.json."
              />
            }
          />
          <Route
            path="map"
            element={
              <Stub
                name="Map"
                hint="Use react-leaflet's MapContainer + CircleMarker over /static/data/map-{window}.json. Click a marker to load a side panel from establishments-{window}.json filtered by zip."
              />
            }
          />
          <Route
            path="establishments"
            element={
              <Stub
                name="Browse"
                hint="Load /static/data/establishments-{window}.json. useState for filter inputs synced to URL via useSearchParams; useMemo to derive sorted+filtered+paginated rows."
              />
            }
          />
          <Route
            path="ops"
            element={
              <Stub
                name="Pipeline"
                hint="Fetch /static/data/ops.json and render the runs + counts tables."
              />
            }
          />
          <Route
            path="establishment/:id"
            element={
              <Stub
                name="Establishment detail"
                hint="useParams() to read :id, fetch /static/data/establishment/:id.json. Inspection history Line, stacked-bar revenue, TABC license table."
              />
            }
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
