BEGIN TRANSACTION;
    DELETE FROM plume
        WHERE date_part('year', report_date) = {{ dag_run.conf.get('year', '2018') }};

    INSERT INTO plume(geometry, report_date, density)
        SELECT geometry, report_date, density
        FROM geopandas;
COMMIT;