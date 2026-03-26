import { FormEvent, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";

import { api, ExportJob, IngestJob, SearchHit } from "./api";
import { Badge, Button, Card, CardBody, CardTitle, Input, ProgressBar } from "./components/ui";

function formatNumber(value: number): string {
  return new Intl.NumberFormat().format(value);
}

function formatBytes(value: number): string {
  if (!value) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let index = 0;
  let current = value;
  while (current >= 1024 && index < units.length - 1) {
    current /= 1024;
    index += 1;
  }
  return `${current.toFixed(current >= 100 ? 0 : 1)} ${units[index]}`;
}

function statusTone(status?: string): string {
  if (!status) return "neutral";
  if (status === "completed") return "good";
  if (status === "failed") return "bad";
  if (status === "running") return "live";
  return "neutral";
}

export default function App() {
  const [sourceBucket, setSourceBucket] = useState("raw-combos");
  const [sourceObject, setSourceObject] = useState("raw/your-file.txt");
  const [autoMerge, setAutoMerge] = useState(true);
  const [ingestJobId, setIngestJobId] = useState<string>("");
  const [mergeJobId, setMergeJobId] = useState<string>("");

  const [searchQuery, setSearchQuery] = useState("");
  const [searchUrl, setSearchUrl] = useState("");
  const [searchUser, setSearchUser] = useState("");
  const [searchLimit, setSearchLimit] = useState(100);
  const [hits, setHits] = useState<SearchHit[]>([]);
  const [searchTookMs, setSearchTookMs] = useState<number>(0);

  const [exportLimit, setExportLimit] = useState(100000);
  const [exportJobId, setExportJobId] = useState<string>("");
  const [downloadUrl, setDownloadUrl] = useState("");

  const dashboardQuery = useQuery({
    queryKey: ["dashboard"],
    queryFn: api.dashboard,
    refetchInterval: 4000,
  });

  const ingestQuery = useQuery({
    queryKey: ["ingest", ingestJobId],
    queryFn: () => api.getIngest(ingestJobId),
    enabled: Boolean(ingestJobId),
    refetchInterval: ({ state }) => {
      const data = state.data as IngestJob | undefined;
      if (!data) return 2000;
      if (data.status === "queued" || data.status === "running") return 2000;
      return false;
    },
  });

  const mergeQuery = useQuery({
    queryKey: ["merge", mergeJobId],
    queryFn: () => api.getMerge(mergeJobId),
    enabled: Boolean(mergeJobId),
    refetchInterval: ({ state }) => {
      const data = state.data as { status: string } | undefined;
      if (!data) return 3000;
      if (data.status === "queued" || data.status === "running") return 3000;
      return false;
    },
  });

  const exportQuery = useQuery({
    queryKey: ["export", exportJobId],
    queryFn: () => api.getExport(exportJobId),
    enabled: Boolean(exportJobId),
    refetchInterval: ({ state }) => {
      const data = state.data as ExportJob | undefined;
      if (!data) return 3000;
      if (data.status === "queued" || data.status === "running") return 3000;
      return false;
    },
  });

  const createIngest = useMutation({
    mutationFn: api.createIngest,
    onSuccess: (job) => {
      setIngestJobId(job.id);
      setMergeJobId("");
    },
  });

  const createMerge = useMutation({
    mutationFn: api.createMerge,
    onSuccess: (job) => setMergeJobId(job.id),
  });

  const searchMutation = useMutation({
    mutationFn: api.search,
    onSuccess: (response) => {
      setHits(response.hits);
      setSearchTookMs(response.took_ms);
    },
  });

  const createExport = useMutation({
    mutationFn: api.createExport,
    onSuccess: (job) => {
      setExportJobId(job.id);
      setDownloadUrl("");
    },
  });

  const fetchDownload = useMutation({
    mutationFn: api.getExportDownload,
    onSuccess: (payload) => setDownloadUrl(payload.download_url),
  });

  const ingestProgress = useMemo(() => {
    const job = ingestQuery.data;
    if (!job || !job.source_size_bytes) return 0;
    return Math.min((job.checkpoint_offset / job.source_size_bytes) * 100, 100);
  }, [ingestQuery.data]);

  function handleCreateIngest(event: FormEvent) {
    event.preventDefault();
    createIngest.mutate({
      source_bucket: sourceBucket,
      source_object: sourceObject,
      auto_merge: autoMerge,
    });
  }

  function handleSearch(event: FormEvent) {
    event.preventDefault();
    searchMutation.mutate({
      query: searchQuery,
      filter_url: searchUrl || undefined,
      filter_username: searchUser || undefined,
      limit: searchLimit,
      prefix: true,
      typo_tolerance: true,
    });
  }

  function handleCreateExport(event: FormEvent) {
    event.preventDefault();
    createExport.mutate({
      query: searchQuery,
      filter_url: searchUrl || undefined,
      filter_username: searchUser || undefined,
      line_limit: exportLimit,
    });
  }

  return (
    <div className="page">
      <header className="hero">
        <p className="eyebrow">VortexVault v2</p>
        <h1>Maximum Performance Control Plane</h1>
        <p>
          Streaming ingest + sharded search optimized for single-host Proxmox deployments.
          PostgreSQL stores metadata only; search payload stays in Meilisearch shards.
        </p>
      </header>

      <section className="grid metrics">
        <Card>
          <CardTitle>Active Ingest</CardTitle>
          <CardBody>
            <strong>{formatNumber(dashboardQuery.data?.active_ingest_jobs ?? 0)}</strong>
          </CardBody>
        </Card>
        <Card>
          <CardTitle>Active Merge</CardTitle>
          <CardBody>
            <strong>{formatNumber(dashboardQuery.data?.active_merge_jobs ?? 0)}</strong>
          </CardBody>
        </Card>
        <Card>
          <CardTitle>Active Export</CardTitle>
          <CardBody>
            <strong>{formatNumber(dashboardQuery.data?.active_export_jobs ?? 0)}</strong>
          </CardBody>
        </Card>
        <Card>
          <CardTitle>Dedupe Cardinality</CardTitle>
          <CardBody>
            <strong>{formatNumber(dashboardQuery.data?.dedupe_cardinality_estimate ?? 0)}</strong>
            <p className="muted">HLL estimated unique rows</p>
          </CardBody>
        </Card>
      </section>

      <section className="grid split">
        <Card>
          <CardTitle>Ingest Job</CardTitle>
          <CardBody>
            <form onSubmit={handleCreateIngest} className="form">
              <label>
                Source Bucket
                <Input value={sourceBucket} onChange={(e) => setSourceBucket(e.target.value)} />
              </label>
              <label>
                Source Object Key
                <Input value={sourceObject} onChange={(e) => setSourceObject(e.target.value)} />
              </label>
              <label className="checkbox-row">
                <input type="checkbox" checked={autoMerge} onChange={(e) => setAutoMerge(e.target.checked)} />
                Auto merge after ingest
              </label>
              <Button type="submit" disabled={createIngest.isPending}>
                {createIngest.isPending ? "Scheduling..." : "Start Streaming Ingest"}
              </Button>
            </form>

            {ingestQuery.data ? (
              <div className="job-panel">
                <div className="job-line">
                  <Badge className={statusTone(ingestQuery.data.status)}>{ingestQuery.data.status}</Badge>
                  <span>{ingestQuery.data.id}</span>
                </div>
                <ProgressBar value={ingestProgress} />
                <div className="two-col">
                  <div>Processed: {formatNumber(ingestQuery.data.processed_lines)}</div>
                  <div>Indexed: {formatNumber(ingestQuery.data.indexed_docs)}</div>
                  <div>Duplicates: {formatNumber(ingestQuery.data.duplicate_lines)}</div>
                  <div>Invalid: {formatNumber(ingestQuery.data.invalid_lines)}</div>
                  <div>Checkpoint: {formatBytes(ingestQuery.data.checkpoint_offset)}</div>
                  <div>Total: {formatBytes(ingestQuery.data.source_size_bytes)}</div>
                </div>
                <div className="actions-row">
                  <Button
                    type="button"
                    variant="secondary"
                    disabled={!ingestJobId || createMerge.isPending}
                    onClick={() => createMerge.mutate({ ingest_job_id: ingestJobId })}
                  >
                    Track Merge
                  </Button>
                  {mergeQuery.data ? <Badge className={statusTone(mergeQuery.data.status)}>{mergeQuery.data.status}</Badge> : null}
                </div>
              </div>
            ) : null}
          </CardBody>
        </Card>

        <Card>
          <CardTitle>Search + Export</CardTitle>
          <CardBody>
            <form onSubmit={handleSearch} className="form">
              <label>
                Query
                <Input value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} placeholder="gmail.com" />
              </label>
              <label>
                Filter URL
                <Input value={searchUrl} onChange={(e) => setSearchUrl(e.target.value)} placeholder="optional" />
              </label>
              <label>
                Filter Username
                <Input value={searchUser} onChange={(e) => setSearchUser(e.target.value)} placeholder="optional" />
              </label>
              <label>
                Limit
                <Input
                  type="number"
                  value={searchLimit}
                  onChange={(e) => setSearchLimit(Number(e.target.value) || 100)}
                  min={1}
                  max={5000}
                />
              </label>
              <Button type="submit" disabled={searchMutation.isPending}>
                {searchMutation.isPending ? "Searching..." : "Run Federated Search"}
              </Button>
            </form>

            <div className="job-line">
              <span>{hits.length} rows</span>
              <span>{searchTookMs.toFixed(2)} ms</span>
            </div>
            <div className="results">
              {hits.slice(0, 20).map((hit) => (
                <article key={hit.id} className="result-row">
                  <div>
                    <strong>{hit.url}</strong>
                    <p>{hit.username}</p>
                  </div>
                  <div>
                    <p>{hit.password}</p>
                    <p className="muted">shard {hit.shard}</p>
                  </div>
                </article>
              ))}
            </div>

            <form onSubmit={handleCreateExport} className="form export-form">
              <label>
                Export Line Limit
                <Input
                  type="number"
                  value={exportLimit}
                  onChange={(e) => setExportLimit(Number(e.target.value) || 100000)}
                  min={1}
                  max={50000000}
                />
              </label>
              <div className="actions-row">
                <Button type="submit" disabled={createExport.isPending}>
                  {createExport.isPending ? "Queueing..." : "Queue Async Export"}
                </Button>
                {exportQuery.data ? <Badge className={statusTone(exportQuery.data.status)}>{exportQuery.data.status}</Badge> : null}
              </div>
              {exportQuery.data?.status === "completed" ? (
                <Button type="button" variant="secondary" onClick={() => fetchDownload.mutate(exportQuery.data!.id)}>
                  Get Download URL
                </Button>
              ) : null}
              {downloadUrl ? (
                <a className="download-link" href={downloadUrl} target="_blank" rel="noreferrer">
                  Download Parquet (zstd)
                </a>
              ) : null}
            </form>
          </CardBody>
        </Card>
      </section>

      {(createIngest.error || searchMutation.error || createExport.error || fetchDownload.error) ? (
        <Card className="error-card">
          <CardTitle>Last Error</CardTitle>
          <CardBody>
            <code>
              {(createIngest.error as Error)?.message ||
                (searchMutation.error as Error)?.message ||
                (createExport.error as Error)?.message ||
                (fetchDownload.error as Error)?.message}
            </code>
          </CardBody>
        </Card>
      ) : null}
    </div>
  );
}
