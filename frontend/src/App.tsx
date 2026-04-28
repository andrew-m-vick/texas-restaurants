import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './Layout';
import Overview from './pages/Overview';
import Revenue from './pages/Revenue';
import Inspections from './pages/Inspections';
import Correlation from './pages/Correlation';
import MapPage from './pages/Map';
import Browse from './pages/Browse';
import Ops from './pages/Ops';
import EstablishmentDetail from './pages/EstablishmentDetail';

// Mounted under /app/ in production (see vite.config.ts `base`).
// During dev (vite serve) the basename is '/app' too — the dev server
// rewrites unknown paths back to index.html.
export default function App() {
  return (
    <BrowserRouter basename="/app">
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Overview />} />
          <Route path="revenue" element={<Revenue />} />
          <Route path="inspections" element={<Inspections />} />
          <Route path="correlation" element={<Correlation />} />
          <Route path="map" element={<MapPage />} />
          <Route path="establishments" element={<Browse />} />
          <Route path="ops" element={<Ops />} />
          <Route path="establishment/:id" element={<EstablishmentDetail />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
