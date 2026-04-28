// Central Chart.js registration. Importing this file once (from main.tsx)
// makes every controller, element, scale, and plugin available to every
// react-chartjs-2 component on every page.
import {
  ArcElement,
  BarController,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  DoughnutController,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  LogarithmicScale,
  PointElement,
  ScatterController,
  Title,
  Tooltip,
} from 'chart.js';

ChartJS.register(
  ArcElement,
  BarController,
  BarElement,
  CategoryScale,
  DoughnutController,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  LogarithmicScale,
  PointElement,
  ScatterController,
  Title,
  Tooltip
);

ChartJS.defaults.color = '#8b93a4';
ChartJS.defaults.borderColor = '#262b36';

export const ZIP_COLOR = '#f9844a';
export const PALETTE = [
  '#4cc9f0',
  '#f72585',
  '#7209b7',
  '#3a0ca3',
  '#4361ee',
  '#4895ef',
  '#f9844a',
  '#43aa8b',
  '#f9c74f',
  '#90be6d',
];
