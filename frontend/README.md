# Frontend (Vite + React + TypeScript)

A second-generation frontend for the Austin Restaurant Analytics dashboard. Consumes the same `/static/data/*.json` files emitted by the monthly ETL — Postgres still does the heavy lifting at build time, the SPA just renders the pre-materialized output.

## Why React on top of an already-working vanilla JS site?

The vanilla JS dashboard works fine. This rewrite exists to demonstrate React-shaped engineering decisions on a real, non-trivial dataset: typed data contracts, a routing tree, hooks, side-effect management around imperative chart libraries (Chart.js + Leaflet), build tooling, and a deployment story that ships a static bundle alongside a static-only Python shell.

The two stacks coexist while the port is in progress. The Flask templates serve the legacy UI at `/`, `/revenue`, etc. The React app is mounted at `/app/*`. Once the React side is feature-complete, the legacy templates and `/static/js/charts.js` etc. can be deleted.

## Stack

- **Vite** for dev server + production build
- **React 18** with **TypeScript** (strict mode) — the JSON shapes in `src/types.ts` are the contract between Python and the UI
- **React Router v6** with `BrowserRouter` mounted at `/app`
- **react-chartjs-2** wrapping Chart.js
- **react-leaflet** for the map
- Service worker / PWA install reused from the existing `/static/sw.js`

## Layout

```
frontend/
├── index.html                  # Vite entry — links the shared style.css
├── vite.config.ts              # base: '/app/', outDir: ../app/static/dist
├── tsconfig.json
├── package.json
└── src/
    ├── main.tsx                # ReactDOM root + SW registration
    ├── App.tsx                 # router; one <Route> per legacy tab
    ├── Layout.tsx              # header (brand, search, window select), nav, <Outlet />
    ├── types.ts                # TypeScript shapes for every JSON file
    ├── lib/
    │   ├── data.ts             # fetchData<T>() + useData<T>() hook (cached)
    │   ├── format.ts           # fmtMoney / fmtNumber / fmtScore
    │   └── window.ts           # useWindow() — reads/writes ?window= in URL
    ├── components/
    │   ├── Loading.tsx         # Loading / Empty / ErrorMsg primitives
    │   ├── SearchBox.tsx       # debounced typeahead over /static/data/search.json
    │   └── ZipBarChart.tsx     # shared chart wrapper for ZIP-level bars
    └── pages/
        ├── Overview.tsx
        ├── Revenue.tsx
        ├── Inspections.tsx
        ├── Correlation.tsx
        ├── Map.tsx
        ├── Browse.tsx
        ├── Ops.tsx
        └── EstablishmentDetail.tsx
```

## Local dev

```bash
# In one terminal — Flask has to be running so the SPA can fetch
# /static/data/*.json and the shared CSS.
cd .. && python run.py

# In another — Vite dev server with HMR.
cd frontend
npm install
npm run dev    # http://localhost:5173/app/
```

Vite proxies `/static/*` and `/sw.js` to the Flask server at `:5000`, so the React app pulls the same JSON the legacy templates do.

## Production build

```bash
cd frontend
npm run build
```

Vite writes to `../app/static/dist/`. Flask serves it via the catchall `/app/<path>` route in `app/routes.py`. Railway's build command should run the Vite build before booting Flask — see "Deploy" below.

## Page pattern

Every page follows the same shape — copy [`Overview.tsx`](src/pages/Overview.tsx):

```tsx
import type { SomeData } from '../types';
import { useData, dataUrl } from '../lib/data';
import { useWindow } from '../lib/window';
import { Loading, Empty, ErrorMsg } from '../components/Loading';

export default function MyPage() {
  const [window] = useWindow();
  const state = useData<SomeData>(dataUrl('myendpoint', window));
  if (state.status === 'loading') return <Loading />;
  if (state.status === 'error') return <ErrorMsg error={state.error} />;
  const data = state.data;
  // ...render data...
}
```

### Conventions

- **Don't fetch in `useEffect`** unless you need it. `useData<T>()` already handles loading/error/cancellation and caches by URL.
- **Type every JSON shape in `types.ts`.** This is the single source of truth between Python and the UI.
- **Keep imperative library boundaries narrow.** Wrap Chart.js / Leaflet in component files (`ZipBarChart.tsx` is the model). Don't pepper raw `Chart.js` calls across pages.
- **Mirror the legacy CSS classes** (`.kpi`, `.card`, `.grid-2`, `.empty-msg`, etc.) so the React build inherits the existing visual identity.

## Deploy notes (TODO when port is complete)

1. Add a Railway build step that runs `npm ci && npm run build` in `frontend/` before Flask boots. The simplest way: a top-level `nixpacks.toml` with a custom `cmds` block, or a `Procfile` `release:` line.
2. Once the React app is feature-complete, change `/` (and the other legacy Flask routes) to redirect to `/app/...`. Then delete `app/templates/*.html` and `app/static/js/*.js` (except `sw.js`).
3. Bump `CACHE_VERSION` in `app/static/sw.js` so returning users pull the new shell.
