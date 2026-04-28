// Leaflet map with ZIP-level circle markers + click-driven side panel.
// Establishments JSON is filtered client-side by the selected ZIP.
import { useMemo, useState } from 'react';
import { CircleMarker, MapContainer, TileLayer, Tooltip } from 'react-leaflet';
import type { EstablishmentRow, EstablishmentsData, MapData, MapZip } from '../types';
import { useData, dataUrl } from '../lib/data';
import { useWindow } from '../lib/window';
import { Empty, ErrorMsg, Loading } from '../components/Loading';

type Metric = 'revenue' | 'score';

export default function MapPage() {
  const [window] = useWindow();
  const map = useData<MapData>(dataUrl('map', window));
  const ests = useData<EstablishmentsData>(dataUrl('establishments', window));
  const [metric, setMetric] = useState<Metric>('revenue');
  const [selectedZip, setSelectedZip] = useState<string | null>(null);

  const zips: MapZip[] = map.status === 'ready' ? map.data.zips : [];
  const allEstablishments: EstablishmentRow[] =
    ests.status === 'ready' ? ests.data.rows : [];

  const max = useMemo(() => {
    const vals = zips.map((z) =>
      metric === 'revenue' ? Number(z.total_receipts) : Number(z.avg_score)
    );
    return Math.max(...vals, 1);
  }, [zips, metric]);

  const panelEstablishments = useMemo(() => {
    if (!selectedZip) return [];
    return allEstablishments
      .filter((e) => e.zip === selectedZip)
      .sort(
        (a, b) =>
          (Number(b.avg_monthly_receipts) || 0) -
          (Number(a.avg_monthly_receipts) || 0)
      )
      .slice(0, 50);
  }, [selectedZip, allEstablishments]);

  if (map.status === 'loading' || ests.status === 'loading') return <Loading />;
  if (map.status === 'error') return <ErrorMsg error={map.error} />;
  if (ests.status === 'error') return <ErrorMsg error={ests.error} />;

  return (
    <>
      <h1>Map</h1>
      <div className="controls-row">
        <label>
          <input
            type="radio"
            name="metric"
            value="revenue"
            checked={metric === 'revenue'}
            onChange={() => setMetric('revenue')}
          />{' '}
          Size by revenue
        </label>
        <label>
          <input
            type="radio"
            name="metric"
            value="score"
            checked={metric === 'score'}
            onChange={() => setMetric('score')}
          />{' '}
          Size by inspection score
        </label>
      </div>
      <div className="grid-map" style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '1rem' }}>
        <div className="card" style={{ padding: 0, height: 560 }}>
          <MapContainer
            center={[30.27, -97.74]}
            zoom={11}
            style={{ height: '100%', width: '100%', borderRadius: 12 }}
          >
            <TileLayer
              attribution="&copy; OpenStreetMap"
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              maxZoom={19}
            />
            {zips.map((z) => {
              const val =
                metric === 'revenue' ? Number(z.total_receipts) : Number(z.avg_score);
              const radius = 6 + 30 * (val / max);
              const color = metric === 'revenue' ? '#ff006e' : '#ffd60a';
              return (
                <CircleMarker
                  key={`${z.city}-${z.zip}`}
                  center={[z.latitude, z.longitude]}
                  radius={radius}
                  pathOptions={{
                    color: '#0b0f1a',
                    fillColor: color,
                    fillOpacity: 0.75,
                    weight: 1.5,
                  }}
                  eventHandlers={{
                    click: () => setSelectedZip(z.zip),
                  }}
                >
                  <Tooltip>
                    <b>ZIP {z.zip}</b>
                    <br />
                    Establishments: {z.establishments}
                    <br />
                    Avg score: {Number(z.avg_score).toFixed(1)}
                    <br />
                    Revenue: ${Number(z.total_receipts).toLocaleString()}
                  </Tooltip>
                </CircleMarker>
              );
            })}
          </MapContainer>
        </div>
        <div id="zipPanel" className="card">
          {!selectedZip ? (
            <Empty label="Click a ZIP marker to see establishments." />
          ) : !panelEstablishments.length ? (
            <>
              <h3>ZIP {selectedZip}</h3>
              <Empty label="No establishments." />
            </>
          ) : (
            <>
              <h3>
                ZIP {selectedZip}{' '}
                <span
                  className="muted"
                  style={{ fontSize: '0.85rem', fontWeight: 'normal' }}
                >
                  · {panelEstablishments.length} establishments
                </span>
              </h3>
              <ul className="zip-list">
                {panelEstablishments.map((e) => (
                  <li key={e.id}>
                    <a href={`/app/establishment/${e.id}`}>
                      <div className="name">{e.canonical_name}</div>
                      <div className="meta">
                        {e.city} · {e.match_method}
                        {e.avg_score != null
                          ? ` · score ${Number(e.avg_score).toFixed(1)}`
                          : ''}
                        {e.avg_monthly_receipts != null
                          ? ` · $${Math.round(
                              Number(e.avg_monthly_receipts)
                            ).toLocaleString()}/mo`
                          : ''}
                      </div>
                    </a>
                  </li>
                ))}
              </ul>
            </>
          )}
        </div>
      </div>
    </>
  );
}
