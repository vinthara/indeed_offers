DROP FUNCTION IF EXISTS scraped_stats();

CREATE OR REPLACE FUNCTION scraped_stats(
	)
    RETURNS json
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
WITH count_jobs_to_scrap AS
(
	SELECT COUNT(*) AS to_scrap FROM jobs WHERE description_verbose IS NULL
),
count_jobs_scraped AS
(
	SELECT COUNT(*) AS scraped FROM jobs WHERE description_verbose IS NOT NULL
),
total_jobs AS
(
	SELECT COUNT(*) AS count_jobs FROM jobs
)
SELECT JSON_BUILD_OBJECT(
	'scraped', scraped,
	'total_jobs', count_jobs,
	'scrap_progress', ROUND(scraped::NUMERIC / count_jobs, 2),
	'to_scrap' , to_scrap
) FROM count_jobs_to_scrap, count_jobs_scraped, total_jobs;
$BODY$;

COMMENT ON FUNCTION scraped_stats()
    IS 'Get scraping advancment stats';