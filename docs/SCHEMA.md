# Database Schema

## processing_jobs
Tracks cleaner/upload jobs and progress.

Main columns:
- `id` UUID PK
- `job_type` enum: `clean|upload`
- `status` enum: `pending|queued|running|paused|completed|failed`
- `source_path`, `output_file`
- counters: `processed_lines`, `unique_found`, `rows_inserted`, `rows_skipped`
- execution state: `current_file`, `current_line`, `pause_requested`, `error_message`, `meta`

## file_checkpoints
Checkpoint state for resumable streaming.

Main columns:
- `job_id` FK -> `processing_jobs`
- `file_path`
- `encoding`
- `position` (text stream position cookie)
- `processed_lines`

Unique constraint:
- `(job_id, file_path)`

## combo_entries
Main searchable credential table.

Main columns:
- `id` BIGINT PK
- `url`, `username`, `password`
- `digest` SHA-256 bytes

Unique constraint:
- `(digest, url, username, password)`

Indexes:
- `GIN` trigram index on `url`
- `GIN` trigram index on `username`
- `GIN` trigram index on `password`
- composite B-Tree index `(url, username, password)`
