CREATE TABLE IF NOT EXISTS plume (
    id serial,
    geometry  geography(MultiPolygon,4326),
    report_date date,
    density smallint
);

create index if not exists plume_day
	on plume (report_date);
