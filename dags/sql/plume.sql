CREATE TABLE IF NOT EXISTS plume_stage (
    id serial,
    geometry geography(MultiPolygon),
    report_date date,
    density smallint
);

create index if not exists plume_day
	on plume_stage (report_date);
