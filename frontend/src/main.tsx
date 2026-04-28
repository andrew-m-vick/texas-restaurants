import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './lib/charts-setup';

const root = document.getElementById('root');
if (!root) throw new Error('missing #root');
ReactDOM.createRoot(root).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

// Service worker registration: the Flask app already serves /sw.js
// from site root. The React build is served from /app/, so the SW
// scope still controls every URL.
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/sw.js').catch(() => undefined);
  });
}
