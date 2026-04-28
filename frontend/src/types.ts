// Shapes of the static JSON files emitted by pipeline/export/static_json.py.
// Keep this in sync with that script — it's the contract between the
// build-time Python and the runtime React app.

export type Window = '12m' | '3y' | '5y' | 'all';

export interface OverviewKpis {
  establishments: number;
  avg_score: number | null;
  total_receipts: number;
  inspections: number;
}

export interface ZipReceipts {
  city: string;
  zip: string;
  receipts: number;
}

export interface ZipScore {
  city: string;
  zip: string;
  avg_score: number;
}

export interface CityKpi {
  city: string;
  establishments: number;
  avg_score: number | null;
  total_receipts: number;
  inspections: number;
}

export interface OverviewData {
  kpis: OverviewKpis;
  by_city: CityKpi[];
  top_zips: ZipReceipts[];
  bottom_zips: ZipScore[];
}

export interface RevenueMonthly {
  month: string;
  city: string;
  total: number;
}

export interface RevenueByZip {
  city: string;
  zip: string;
  total: number;
}

export interface RevenueData {
  monthly: RevenueMonthly[];
  by_zip: RevenueByZip[];
}

export interface ScoreBucket {
  score_bucket: string;
  inspections: number;
}

export interface RepeatOffender {
  establishment_id: number;
  city: string;
  canonical_name: string;
  zip: string | null;
  inspection_count: number;
  low_score_count: number;
  avg_score: number | null;
  min_score: number | null;
}

export interface InspectionsData {
  distribution: ScoreBucket[];
  repeat_offenders: RepeatOffender[];
}

export interface CorrelationPoint {
  establishment_id: number;
  city: string;
  canonical_name: string;
  zip: string | null;
  avg_score: number;
  avg_monthly_receipts: number;
  match_score: number | null;
}

export interface CorrelationData {
  points: CorrelationPoint[];
}

export interface MapZip {
  city: string;
  zip: string;
  establishments: number;
  avg_score: number;
  total_receipts: number;
  latitude: number;
  longitude: number;
}

export interface MapData {
  zips: MapZip[];
}

export interface EstablishmentRow {
  id: number;
  canonical_name: string;
  canonical_address: string | null;
  city: string;
  zip: string | null;
  match_method: string;
  match_score: number | null;
  inspection_count: number | null;
  avg_score: number | null;
  avg_monthly_receipts: number | null;
}

export interface EstablishmentsData {
  rows: EstablishmentRow[];
}

export interface SearchResult {
  id: number;
  city: string;
  canonical_name: string;
  canonical_address: string | null;
  zip: string | null;
  match_method: string;
}

export interface SearchData {
  results: SearchResult[];
}

export interface InspectionEntry {
  inspection_date: string;
  score: number | null;
  inspection_type: string | null;
}

export interface RevenueEntry {
  month: string;
  total_receipts: number;
  liquor_receipts: number;
  wine_receipts: number;
  beer_receipts: number;
}

export interface LicenseEntry {
  license_id: string;
  license_type: string | null;
  tier: string | null;
  primary_status: string | null;
  original_issue_date: string | null;
  current_issued_date: string | null;
  expiration_date: string | null;
  status_change_date: string | null;
  gun_sign: string | null;
  master_file_id: string | null;
  owner: string | null;
}

export interface EstablishmentDetail {
  header: {
    id: number;
    canonical_name: string;
    canonical_address: string | null;
    city: string;
    zip: string | null;
    match_method: string;
    match_score: number | null;
  };
  inspections: InspectionEntry[];
  revenue: RevenueEntry[];
  violations: unknown[];
  licenses: LicenseEntry[];
}

export interface PipelineRun {
  dag_id: string;
  layer: string;
  started_at: string | null;
  finished_at: string | null;
  status: string;
  rows_written: number | null;
  notes: string | null;
}

export interface OpsCount {
  tbl: string;
  n: number;
}

export interface OpsData {
  runs: PipelineRun[];
  counts: OpsCount[];
}

export interface LifecycleData {
  status: { status: string; n: number }[];
  gun_sign: { gun_sign: string; n: number }[];
  tenure_vs_score: {
    establishment_id: number;
    canonical_name: string;
    zip: string | null;
    first_issued: string | null;
    tenure_years: number;
    avg_score: number;
    inspection_count: number;
  }[];
  status_by_zip: {
    zip: string;
    total: number;
    active: number;
    expired: number;
    cancelled: number;
  }[];
}
