export type Dashboard = {
  active_ingest_jobs: number;
  active_merge_jobs: number;
  active_export_jobs: number;
  total_completed_ingest: number;
  dedupe_cardinality_estimate: number;
  shard_count: number;
};

export type IngestJob = {
  id: string;
  status: string;
  source_bucket: string;
  source_object: string;
  checkpoint_offset: number;
  source_size_bytes: number;
  processed_lines: number;
  indexed_docs: number;
  invalid_lines: number;
  duplicate_lines: number;
  shard_counts: Record<string, number>;
  metadata_json: Record<string, unknown>;
  error_message: string | null;
  created_at: string;
  started_at: string | null;
  last_checkpoint_at: string | null;
  finished_at: string | null;
};

export type MergeJob = {
  id: string;
  status: string;
  ingest_job_id: string;
  bloom_cardinality_estimate: number;
  cleaned_objects: number;
  notes: string | null;
  error_message: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
};

export type ExportJob = {
  id: string;
  status: string;
  object_key: string | null;
  exported_rows: number;
  error_message: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
};

export type SearchHit = {
  id: string;
  url: string;
  username: string;
  password: string;
  score: number;
  shard: number;
};

export type SearchResponse = {
  took_ms: number;
  total_hits: number;
  hits: SearchHit[];
};

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "/api/v2";

async function http<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}

export const api = {
  dashboard: () => http<Dashboard>("/dashboard"),
  createIngest: (payload: { source_bucket: string; source_object: string; auto_merge: boolean }) =>
    http<IngestJob>("/ingest/jobs", { method: "POST", body: JSON.stringify(payload) }),
  getIngest: (jobId: string) => http<IngestJob>(`/ingest/jobs/${jobId}`),
  createMerge: (payload: { ingest_job_id: string }) => http<MergeJob>("/merge/jobs", { method: "POST", body: JSON.stringify(payload) }),
  getMerge: (jobId: string) => http<MergeJob>(`/merge/jobs/${jobId}`),
  search: (payload: { query: string; filter_url?: string; filter_username?: string; limit: number; prefix: boolean; typo_tolerance: boolean }) =>
    http<SearchResponse>("/search/query", { method: "POST", body: JSON.stringify(payload) }),
  createExport: (payload: { query: string; filter_url?: string; filter_username?: string; line_limit: number }) =>
    http<ExportJob>("/exports", { method: "POST", body: JSON.stringify(payload) }),
  getExport: (jobId: string) => http<ExportJob>(`/exports/${jobId}`),
  getExportDownload: (jobId: string) => http<{ download_url: string }>(`/exports/${jobId}/download`),
};
