-- Existing W02 installations only. Run as a MariaDB account with GRANT OPTION.
-- W09 returns only availability booleans for these two columns, never their paths.
-- No password changes, no write permissions, no DDL or data modifications.
GRANT SELECT (quality_report_path) ON retail_analytics.data_batch TO 'retail_reader'@'localhost';
GRANT SELECT (log_path) ON retail_analytics.task_run TO 'retail_reader'@'localhost';
