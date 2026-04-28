import { NavLink, Outlet } from 'react-router-dom';
import { useWindow, WINDOWS } from './lib/window';
import SearchBox from './components/SearchBox';

// Paths are relative to the BrowserRouter basename ('/app'); React
// Router prepends the basename automatically, so '/' here resolves to
// '/app/' in the URL bar.
const TABS = [
  { to: '/', label: 'Overview', end: true },
  { to: '/revenue', label: 'Revenue' },
  { to: '/inspections', label: 'Inspections' },
  { to: '/correlation', label: 'Correlation' },
  { to: '/map', label: 'Map' },
  { to: '/establishments', label: 'Browse' },
  { to: '/ops', label: 'Pipeline' },
];

export default function Layout() {
  const [window, setWindow] = useWindow();

  return (
    <>
      <header>
        <div className="header-top">
          <div className="brand">
            <img src="/static/favicon.svg" alt="" className="brand-logo" />
            <span>Austin Restaurant Analytics</span>
          </div>
          <div className="header-controls">
            <SearchBox />
            <select
              className="hdr-select"
              title="Time window"
              value={window}
              onChange={(e) => setWindow(e.target.value as typeof window)}
            >
              {WINDOWS.map(({ key, label }) => (
                <option key={key} value={key}>
                  {label}
                </option>
              ))}
            </select>
          </div>
        </div>
        <nav>
          {TABS.map(({ to, label, end }) => (
            <NavLink key={to} to={to} end={end}>
              {label}
            </NavLink>
          ))}
        </nav>
      </header>
      <main>
        <Outlet />
      </main>
    </>
  );
}
