-- Supabase SQL Schema

-- 1) Create table
CREATE TABLE IF NOT EXISTS commits (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_type text NOT NULL,
  commit_sha text NOT NULL,
  commit_timestamp timestamptz NOT NULL,
  repo_name text NOT NULL,
  author text NOT NULL,
  branch text NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT now()
);







-- 2) Unique index to prevent duplicates by repo_name + commit_sha
CREATE UNIQUE INDEX IF NOT EXISTS commits_repo_commit_sha_uniq_idx
  ON commits (repo_name, commit_sha);

-- 3) (Optional) Prevent accidental updates/deletes for non-superuser roles:
-- If you want the table to be strictly append-only for everyone except the
-- database owner / service_role, create RLS and policies or deny UPDATE/DELETE
-- via a policy. Since you stated you'll use the service role key, simple approach:
ALTER TABLE commits ENABLE ROW LEVEL SECURITY;

-- Allow the service_role (bypasses RLS) to do anything. For authenticated users
-- you can explicitly deny updates/deletes (this example denies all operations
-- except INSERT to 'authenticated' role and still lets service_role bypass).
-- Grant INSERT to authenticated and create a policy that only allows INSERTs
GRANT INSERT ON commits TO authenticated;

CREATE POLICY commits_allow_insert_authenticated
  ON commits
  FOR INSERT
  TO authenticated
  WITH CHECK (true);

-- Deny SELECT/UPDATE/DELETE for authenticated by not creating policies for them.
-- The service_role key bypasses RLS, so your backend using service role can do any action.