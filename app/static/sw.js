// Service worker for Austin Restaurant Analytics.
//
// Strategy: precache the app shell on install; use stale-while-revalidate
// at runtime so pages and JSON data files load instantly from cache and
// update in the background. Bump CACHE_VERSION whenever the shell changes.

const CACHE_VERSION = 'v1';
const CACHE_NAME = `austin-rest-${CACHE_VERSION}`;

const SHELL = [
  '/',
  '/revenue',
  '/inspections',
  '/correlation',
  '/map',
  '/establishments',
  '/ops',
  '/static/css/style.css',
  '/static/js/util.js',
  '/static/js/search.js',
  '/static/js/charts.js',
  '/static/js/map.js',
  '/static/js/establishments.js',
  '/static/favicon.svg',
  '/static/manifest.webmanifest',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) =>
      // Use individual fetches so one 404 doesn't abort the whole precache.
      Promise.all(
        SHELL.map((url) =>
          fetch(url, { cache: 'no-cache' })
            .then((resp) => resp.ok ? cache.put(url, resp) : null)
            .catch(() => null)
        )
      )
    )
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);
  // Only intercept same-origin requests. Third-party CDN assets
  // (Chart.js, Leaflet, tile images) keep their own HTTP caching.
  if (url.origin !== self.location.origin) return;

  event.respondWith(
    caches.open(CACHE_NAME).then(async (cache) => {
      const cached = await cache.match(request);
      const network = fetch(request)
        .then((resp) => {
          if (resp && resp.ok) cache.put(request, resp.clone());
          return resp;
        })
        .catch(() => cached);
      // Serve cached immediately if we have it; either way update in the bg.
      return cached || network;
    })
  );
});
