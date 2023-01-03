DROP FUNCTION IF EXISTS update_jobs_updated_at;

CREATE TRIGGER update_jobs_updated_at BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION update_timestamp_on_updates();