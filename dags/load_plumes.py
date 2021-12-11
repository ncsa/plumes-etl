import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

with DAG(
    dag_id="plumes_dag",
    description="Load Plumes data",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval=None,
    catchup=False,
) as dag:
    create_plume_table = PostgresOperator(
        task_id="CreatePlumeTable",
        postgres_conn_id="smokes-db",
        sql="sql/create_plumes_table.sql",
    )
    list_import_dir = BashOperator(
        task_id='ListImportDir',
        bash_command='ls -lht /usr/local/plumes/{{ dag_run.conf.get("year", "2018") }}/*/*.geojson',
    )

    def load_plumes(year):
        """
        Function to load plumes shapefiles into DB
        :param year:
        :return:
        """
        import geopandas
        import pandas as pd
        from shapely.ops import unary_union
        from airflow.hooks.postgres_hook import PostgresHook
        from sqlalchemy import create_engine, text
        from numpy import datetime64, int8
        import os

        from geopandas import GeoDataFrame
        from geoalchemy2 import Geography
        from sqlalchemy import DateTime, SmallInteger

        def compute_multipolygons(plumes: GeoDataFrame):
            from shapely.geometry import Polygon, MultiPolygon, GeometryCollection
            import logging

            # Process doesn't work if there are now densities in the record. Return
            # an empty GeoDataFrame to skip
            if "Density" not in plumes:
                logging.warning(f"Dataframe lacks a density column")
                logging.info(plumes.dtypes)
                return GeoDataFrame()

            all_densities = []

            # Produce a multipolygon for each smoke density
            for density in plumes.Density.unique():
                polygon = unary_union(
                    plumes[plumes.Density == density]['geometry'].to_list())

                # We need all of the records to contain a MultiPolygon. Fix up other
                # types that can be generated
                if type(polygon) == Polygon:
                    polygon = MultiPolygon([polygon])
                elif type(polygon) == GeometryCollection:
                    # Filter out linear artifacts to make a pure MultiPolygon
                    polygon = MultiPolygon(
                        list(filter(lambda geom: type(geom) == Polygon, polygon.geoms)))

                multipolygon_dataframe = \
                    geopandas.GeoDataFrame(geometry=geopandas.GeoSeries(polygon))

                # Fill in constants from the source dataframe
                multipolygon_dataframe['report_date'] = datetime64(plumes['Date'][0])
                multipolygon_dataframe['density'] = int8(density)

                all_densities.append(multipolygon_dataframe)
            return GeoDataFrame(pd.concat(all_densities, ignore_index=True))

        pghook = PostgresHook(postgres_conn_id="smokes-db")
        engine = create_engine('postgresql+psycopg2://', creator=pghook.get_conn)

        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS geopandas"))

        directory = fr'/usr/local/plumes/{year}'

        # Build a single GeoDataframe with the entire year's mulitpolygons
        all_plumes = GeoDataFrame()
        for subdir, dirs, files in os.walk(directory):
            for f in files:
                if f.endswith(".geojson"):
                    print(f"Processing {f}")
                    plumes = geopandas.read_file(os.path.join(subdir, f))

                    # This was causing errors in Tableau - confirm projection
                    # plumes = plumes.to_crs("EPSG:3395")

                    # Append this day's multipolygons to the year dataframe
                    all_plumes = GeoDataFrame(pd.concat([all_plumes, compute_multipolygons(plumes)]))

        # Write to postgres
        all_plumes.to_postgis(name="geopandas", con=engine, dtype={
            "geometry": Geography,
            "report_data": DateTime,
            "density": SmallInteger
        })

        # Alter the table to make sure Tableau recognizes the geometry column
        with engine.begin() as conn:
            conn.execute(text("alter table geopandas alter column geometry type geography(MultiPolygon) using geometry::geography(MultiPolygon)"))

    load_plumes = PythonVirtualenvOperator(
        task_id="LoadPlumes",
        python_callable=load_plumes,
        op_kwargs={
            "year": '{{ dag_run.conf.get("year", "2018") }}'
        },
        requirements=[
            "geopandas",
            "SQLAlchemy",
            "GeoAlchemy2"
        ]
    )

    copy = PostgresOperator(
        task_id="CopyStagedData",
        postgres_conn_id="smokes-db",
        autocommit=False,
        sql="sql/copy_staged_plume_data.sql",
        params={
            'year': "{{ dag_run.conf.get('year', '2018') }}"
        }
    )
    [create_plume_table, list_import_dir] >> load_plumes >> copy
