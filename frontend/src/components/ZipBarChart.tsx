// Wraps Chart.js for ZIP-level bar charts. Encapsulates the shared
// visual choices (favicon-orange fill, click-to-drill-into-Browse).
import { Bar } from 'react-chartjs-2';
import { useNavigate } from 'react-router-dom';
import { ZIP_COLOR } from '../lib/charts-setup';

interface Row {
  zip: string;
  value: number;
}

export default function ZipBarChart({
  rows,
  label,
  horizontal = false,
}: {
  rows: Row[];
  label: string;
  horizontal?: boolean;
}) {
  const navigate = useNavigate();
  return (
    <Bar
      data={{
        labels: rows.map((r) => r.zip),
        datasets: [{ label, data: rows.map((r) => r.value), backgroundColor: ZIP_COLOR }],
      }}
      options={{
        maintainAspectRatio: false,
        indexAxis: horizontal ? 'y' : 'x',
        plugins: { legend: { display: false } },
        onClick: (_evt, active) => {
          if (!active.length) return;
          const r = rows[active[0].index];
          navigate(`/establishments?zip=${r.zip}`);
        },
        onHover: (evt, active) => {
          const target = evt.native?.target as HTMLElement | null;
          if (target) target.style.cursor = active.length ? 'pointer' : 'default';
        },
      }}
    />
  );
}
