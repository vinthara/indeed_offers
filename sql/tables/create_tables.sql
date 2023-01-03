CREATE TABLE jobs (
    id character varying(64) NOT NULL,
    job_title character varying(256),
    company_name character varying(64),
    description_snippet text,
    description_verbose text,
    company_rating double precision,
    scraped_at timestamp without time zone,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    url character varying(256),
    PRIMARY KEY (id)
);

CREATE TABLE tags (
    id_job character varying(64),
    job_tag character varying(64),
    FOREIGN KEY (id_job) REFERENCES jobs(id)
);