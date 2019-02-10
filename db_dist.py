'''
read column values of a table in a given date range
convert to numeric values
calculate the percentiles on normalized data

code uses a SQL-data-pipeline
the syntax is
    sql_N(sql_N-1(..(sql_1))
derived table from a derived table from a derived table ..
 
hard-coded values like 0.1, 0.000000000001, -1.0 are used for debugging
'''

from google.cloud import bigquery
import numpy as np
import pandas as pd

import db_diff as db
from config import config, load_config
from lib import make_gen_csv, log_info
import bq_lib as bq
import rs


#
# --> RedShift
#
def rs_configure(options):
    project = options['PROJECT']
    settings = config[project]['rs']
    schema = project
    sample_size = 10000000
    precision = 12
    number_percentiles = 10
    substr_size = 5
    hash_size = 15
    time_columns = 'timestamp'

    aws_json = '../../etc/{proj}/aws.json'.format(proj=project)
    aws_config = load_config(aws_json)
    engine = rs.connect(project, aws_config)
    conn = engine.connect()
    conn.execute("SET search_path TO {schema}".format(schema=schema))

    csv_tables = "rs_{project}_columns_minmax_day.csv".format(project=project)
    csv_ptiles = "rs_{project}_table_column_ptiles.csv".format(project=project)
    csv_basic = "rs_{project}_table_column_basic.csv".format(project=project)

    rs_query = rs.make_run(engine, schema)
    list_schema = rs_make_read_column_type(rs_query, schema)
    schema = {(table_name, column_name): column_type
              for table_name, column_name, column_type in list_schema()}

    convert_string_sql = rs_make_convert_string_sql(substr_size, hash_size)
    read_column_sql = rs_make_read_column_daily_sql(time_columns)
    read_normed_percentiles_sql = rs_make_read_normed_percentiles_sql(number_percentiles)
    read_percentiles = rs_make_read_percentiles(conn, schema, number_percentiles,
                                                  convert_string_sql,
                                                  read_column_sql, read_normed_percentiles_sql)
    read_basic_stats = rs_make_read_basic_stats(conn, schema, read_column_sql)
    gen_tables = make_gen_csv(csv_tables)

    inject = {'csv_ptiles': csv_ptiles,
              'csv_basic': csv_basic,
              'gen_tables': gen_tables,
              'read_column_percentiles': read_percentiles,
              'read_column_basic_stats': read_basic_stats}
    return inject


def rs_make_read_percentiles(conn, schema, number_percentiles,
                               convert_string_sql,
                               read_column_daily_sql, read_normed_percentiles_sql):
    def read_percentiles(table, column, start_day, end_day):
        column_type = schema[(table, column)]
        convert = {'bigint': _rs_convert_number_sql,
                   'smallint': _rs_convert_number_sql,
                   'integer': _rs_convert_number_sql,
                   'real': _rs_convert_number_sql,
                   'double precision': _rs_convert_number_sql,
                   'character': convert_string_sql,
                   'character varying': convert_string_sql,
                   'boolean': _rs_convert_bool_sql,
                   'timestamp with time zone': _rs_convert_time_sql,
                   'timestamp without time zone': _rs_convert_time_sql,
                   'date': _rs_convert_time_sql,
                  }[column_type]
        sql_with = ','.join([
                        read_column_daily_sql(table, column, start_day, end_day),
                        convert(),
                        _rs_normalize_sql()])
        df = pd.read_sql(read_normed_percentiles_sql(sql_with), con=conn)
        df= pd.DataFrame({
            'ptile': np.linspace(0.0, 1.0, num=number_percentiles+1),
            'ptile_value': list(df.iloc[0])
            })
        return df
    return read_percentiles


def rs_make_read_basic_stats(conn, schema, read_column_daily_sql):
    def read_basic_stats(table_name, column, start_day, end_day):
        column_type = schema[(table_name, column)]
        if column_type == 'boolean':
            convert = _rs_basic_bool_sql
        else:
            convert = _rs_basic_identity_sql
        sql_with = ','.join([read_column_daily_sql(table_name, column, start_day, end_day),
                            convert()])
        df = pd.read_sql(_rs_read_basic_stats_sql(sql_with), con = conn)
        return df
    return read_basic_stats


def _rs_basic_bool_sql():
    return """
    derived_basic_stats AS (
        SELECT
            CASE event_column
            WHEN true  THEN 1
            WHEN false THEN 0
            END AS event_column
        FROM derived_base
        )
    """


def _rs_basic_identity_sql():
    return """
    derived_basic_stats AS (
        SELECT
            event_column
        FROM derived_base
        )
    """


def _rs_read_column_type_sql(schema):
    return """
    SELECT table_name, column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = '{table_schema}'
    """.format(table_schema=schema)


def rs_make_read_column_type(rs_query, schema):
    def read_column_type():
        return rs_query(_rs_read_column_type_sql(schema))
    return read_column_type


def rs_make_read_column_daily_sample(rs_query, sample_size, time_column):
    def read_column_daily_sample_sql(table, column, start_day, end_day):
        return """
        DROP TABLE IF EXISTS {table}_sample;
        CREATE TEMP TABLE {table}_sample as (
            SELECT {column}
            FROM {table} 
            WHERE date("{timestamp}") BETWEEN '{start_day}' AND '{end_day}'
            ORDER BY random()
            LIMIT {sample_size});

        WITH
        derived_base AS (
            SELECT
                0 AS event_day,
                0 AS event_hour,
                {column} AS event_column
            FROM {table}_sample
            )
        """.format(table=table,
                column=column,
                timestamp=time_column,
                start_day=start_day,
                end_day=end_day,
                sample_size=sample_size)
    return read_column_daily_sample_sql


def rs_crate_read_column_percentile_daily_sample_sql(sample_size, time_column):
    def read_column_percentile_daily_sample_sql(table, column, start_day, end_day):
        return """
        DROP TABLE IF EXISTS {table}_sample;
        CREATE TEMP TABLE {table}_sample as (
            SELECT {column}
            FROM {table} 
            WHERE date("{timestamp}") BETWEEN '{start_day}' AND '{end_day}'
            ORDER BY random()
            LIMIT {sample_size});

        WITH
        outlier_percentiles AS (
            SELECT
                percentile_cont(0.05) within group (order by {column}) as ptile_lower,
                percentile_cont(0.95) within group (order by {column}) as ptile_upper
            FROM {table}_sample
            ),

        derived_base AS (
            SELECT
                0 as event_day,
                0 as event_hour,
                {column} AS event_column
            FROM {table}_sample
            AND {column} > (SELECT ptile_lower FROM outlier_percentiles)
            AND {column} < (SELECT ptile_upper FROM outlier_percentiles)
            )
        """.format(table=table,
                column=column,
                timestamp=time_column,
                start_day=start_day,
                end_day=end_day,
                sample_size=sample_size)
    return read_column_percentile_daily_sample_sql


def rs_make_read_column_daily_sql(time_column):
    def read_column_daily_sql(table, column, start_day, end_day):
        return """
        WITH
        derived_base AS (
            SELECT
                to_char("{timestamp}", 'YYYYMMDD') as event_day,
                extract(hour from "{timestamp}") as event_hour,
                {column} AS event_column
            FROM {table}
            WHERE date("{timestamp}") BETWEEN '{start_day}' AND '{end_day}'
            )
        """.format(table=table,
                column=column,
                timestamp=time_column,
                start_day=start_day,
                end_day=end_day)
    return read_column_daily_sql


def rs_make_read_column_percentile_daily_sql(time_column):
    def read_column_percentile_daily_sql(table, column, start_day, end_day):
        return """
        WITH
        outlier_percentiles AS (
            SELECT
                percentile_cont(0.05) within group (order by {column}) as ptile_lower,
                percentile_cont(0.95) within group (order by {column}) as ptile_upper
            FROM {table}
            WHERE date("{timestamp}") BETWEEN '{start_day}' AND '{end_day}'
            ),

        derived_base AS (
            SELECT
                to_char("{timestamp}", 'YYYYMMDD') as event_day,
                extract(hour from "{timestamp}") as event_hour,
                {column} AS event_column
            FROM {table}
            WHERE date("{timestamp}") BETWEEN '{start_day}' AND '{end_day}'
            AND {column} > (SELECT ptile_lower FROM outlier_percentiles)
            AND {column} < (SELECT ptile_upper FROM outlier_percentiles)
            )
        """.format(table=table,
                column=column,
                timestamp=time_column,
                start_day=start_day,
                end_day=end_day)
    return read_column_percentile_daily_sql


def _rs_convert_bool_sql():
    return """
    derived_number AS (
        SELECT
            event_day,
            event_hour,
            CASE event_column
            WHEN true  THEN 1
            WHEN false THEN 0
            END AS number_column
        FROM derived_base
        )
    """


def _rs_convert_time_sql():
    return """
    derived_number AS (
        SELECT
            event_day,
            event_hour,
            extract(epoch from event_column) AS number_column
        FROM derived_base
        )
    """


def _rs_convert_number_sql():
    return """
    derived_number AS (
        SELECT
            event_day,
            event_hour,
            event_column AS number_column
        FROM derived_base
        )
    """


def rs_make_convert_string_sql(slice_size, hash_size):
    def convert_string_sql():
        if slice_size >= 0:
            substr_parameters = "event_column,0,{no_characters}".format(no_characters=slice_size)
        else:
            substr_parameters = "reverse(event_column),0,{no_characters}".format(no_characters=(-1*slice_size))

        return """
        string_substr AS (
            SELECT
                event_day,
                event_hour,
                SUBSTRING({parameters}) AS string_column
            FROM derived_base
            ),

        derived_number AS (
            SELECT
                event_day,
                event_hour,
                STRTOL(SUBSTRING(MD5(string_column),0,{no_characters}), 16) AS number_column
            FROM string_substr
            )
        """.format(parameters=substr_parameters, no_characters=hash_size)
    return convert_string_sql


def _rs_convert_hash_sql():
    return """
    derived_number AS (
        SELECT
            event_day,
            event_hour,
            STRTOL(SUBSTRING(event_column,0,15), 16) AS number_column
        FROM derived_base
        WHERE regexp_count(event_column, '[ABCDEF0123456789]') = length(event_column)
        )
    """


def _rs_normalize_sql():
    return """
    derived_stats AS (
        SELECT
            min(number_column) AS min_value,
            max(number_column) AS max_value,
            max(number_column) - min(number_column) AS range_value
        FROM derived_number
        WHERE number_column >= 0
        ),

    derived_norm AS (
        SELECT
            event_day,
            event_hour,
            number_column as raw_value,
            CASE
            WHEN (SELECT range_value from derived_stats) = 0
                THEN 0.1
            WHEN (number_column::float - (SELECT min_value from derived_stats)) = 0
                THEN 0.000000000001
            ELSE
                (number_column::float - (SELECT min_value from derived_stats))
                / (SELECT range_value from derived_stats)
            END AS normed_value
        FROM derived_number
        WHERE number_column >= 0
        )
    """


def _rs_read_normed_sql(sql_with):
    return """
    {sql_with}

    SELECT
        event_day,
        event_hour,
        raw_value,
        normed_value
    FROM derived_norm
    """.format(sql_with=sql_with)


def rs_make_read_normed_percentiles_sql(number_percentiles):
    def read_normed_percentiles_sql(sql_with):
        pcont = []
        for tile in np.linspace(0.0, 1.0, num=number_percentiles+1):
            pcont.append("""
                    NVL(percentile_cont({ptile:.2f})
                        within group (order by normed_value), -1.0) as ptile_value{tile_id:02d}
                    """.format(ptile=tile, tile_id=int(tile*100)))
        sql_ptile = ', '.join(pcont)

        sql = """
            {sql_with}

            SELECT
            {ptiles}
            FROM derived_norm
            """.format(ptiles=sql_ptile, sql_with=sql_with)
        return sql
    return read_normed_percentiles_sql


def _rs_read_basic_stats_sql(sql_with):
    return """
    {sql_with}

    SELECT
        min(event_column) AS min_value,
        max(event_column) AS max_value,
        count(event_column) AS non_null_count,
        sum(NVL2(event_column, 0, 1)) AS null_count,
        count(distinct event_column) AS distinct_count
    FROM derived_basic_stats
    """.format(sql_with=sql_with)


#
# --> BigQuery
#

def bq_configure(options):
    project = options['PROJECT']
    settings = config[project]['bq']
    number_percentiles = 10
    # adjust size to match Redshift substring()
    substr_size = 5-1
    hash_size = 15-1
    time_columns = 'timestamp'
    extend_search = 0

    gcp_cfg = '../../etc/{proj}/gcp.json'.format(proj=project)
    gc_client = bigquery.Client.from_service_account_json(gcp_cfg)
    dataset = gc_client.dataset(settings['dataset'])

    csv_tables = "bq_{project}_columns_minmax_day.csv".format(project=project)
    csv_ptiles = "bq_{project}_table_column_ptiles.csv".format(project=project)
    csv_basic = "bq_{project}_table_column_basic.csv".format(project=project)

    list_tables = bq.make_list_tables(dataset, lambda x: x)
    schema = {(table.name, field.name): field.field_type
                for table in list_tables()
                for field in table.schema}

    convert_string_sql = bq_make_convert_string_sql(substr_size, hash_size)
    read_column_daily_sql = bq_make_read_column_daily_sql(time_columns, extend_search)
    read_normed_percentiles_sql = bq_make_read_normed_percentiles_sql(number_percentiles)

    read_percentiles = bq_make_read_percentiles(settings['project'],
                                                  settings['dataset'],
                                                  schema,
                                                  gcp_cfg,
                                                  convert_string_sql,
                                                  read_column_daily_sql,
                                                  read_normed_percentiles_sql)
    read_basic_stats = bq_make_read_basic_stats(settings['project'],
                                                  settings['dataset'],
                                                  gcp_cfg,
                                                  read_column_daily_sql)
    gen_tables = make_gen_csv(csv_tables)

    inject = {
            'csv_ptiles': csv_ptiles,
            'csv_basic': csv_basic,
            'gen_tables': gen_tables,
            'read_column_percentiles': read_percentiles,
            'read_column_basic_stats': read_basic_stats}
    return inject


def bq_make_read_percentiles(project, dataset_name, schema, gcp_key,
                               convert_string_sql,
                               read_column_daily_sql, read_normed_percentiles_sql):
    def read_percentiles(table_name, column, start_day, end_day):
        column_type = schema[(table_name, column)]
        convert = {'INTEGER': _bq_convert_number_sql,
                   'FLOAT': _bq_convert_number_sql,
                   'STRING': convert_string_sql,
                   'TIMESTAMP': _bq_convert_time_sql,
                   'BOOLEAN': _bq_convert_bool_sql,
                  }[column_type]
        table_name = '.'.join([dataset_name, table_name])
        sql_with = ','.join([
                         read_column_daily_sql(table_name, column, start_day, end_day),
                         convert(),
                         _bq_normalize_sql()])
        df = pd.read_gbq(
                    read_normed_percentiles_sql(sql_with),
                    dialect = 'standard',
                    project_id = project,
                    private_key = gcp_key)
        return df
    return read_percentiles


def bq_make_read_basic_stats(project, dataset_name, gcp_key, read_column_daily_sql):
    def read_basic_stats(table_name, column, start_day, end_day):
        table_name = '.'.join([dataset_name, table_name])
        sql_with = ','.join([read_column_daily_sql(table_name, column, start_day, end_day)])
        df = pd.read_gbq(
                    _bq_read_basic_stats_sql(sql_with),
                    dialect = 'standard',
                    project_id = project,
                    private_key = gcp_key)
        return df
    return read_basic_stats


def bq_make_read_column_daily_sql(time_column, extend_search):
    def read_column_daily_sql(table, column, start_day, end_day):
        return """
        derived_base AS (
            SELECT
                format_date('%Y%m%d', DATE({timestamp})) as event_day,
                extract(hour from {timestamp}) as event_hour,
                {column} AS event_column
            FROM {table}
            WHERE DATE({timestamp}) BETWEEN '{start_day}'
                                        AND '{end_day}'
            )
        """.format(table=table, column=column,
            start_day=start_day, end_day=end_day,
            timestamp=time_column,
            extend_days=extend_search)
    return read_column_daily_sql


def bq_make_read_column_percentile_daily_sql(percentiles, time_column, extend_search):
    def _bq_read_column_percentile_daily_sql(table, column, start_day, end_day):
        return """
        percentiles AS (
            SELECT
                APPROX_QUANTILES({column}, {percentiles}) AS ptiles
            FROM {table}
            WHERE _PARTITIONTIME
                BETWEEN TIMESTAMP(datetime_sub(DATETIME('{start_day}'), interval {extend_days} day))
                AND TIMESTAMP(datetime_add(DATETIME('{end_day}'), interval {extend_days} day))
            AND DATE({timestamp}) BETWEEN '{start_day}'
                                      AND '{end_day}'
            ),
        
        outlier_percentiles AS (
            SELECT
                ptiles[OFFSET(1)] AS ptile_lower,
                ptiles[OFFSET(ARRAY_LENGTH(ptiles)-2)] AS ptile_upper
            FROM percentiles
            ),
        
        derived_base AS (
            SELECT
                format_date('%Y%m%d', DATE({timestamp})) as event_day,
                extract(hour from {timestamp}) as event_hour,
                {column} AS event_column
            FROM {table}
            WHERE _PARTITIONTIME
                BETWEEN TIMESTAMP(datetime_sub(DATETIME('{start_day}'), interval {extend_days} day))
                AND TIMESTAMP(datetime_add(DATETIME('{end_day}'), interval {extend_days} day))
            AND DATE({timestamp}) BETWEEN '{start_day}'
                                      AND '{end_day}'
            AND {column} > (SELECT ptile_lower FROM outlier_percentiles)
            AND {column} < (SELECT ptile_upper FROM outlier_percentiles)
            )
        """.format(table=table, column=column,
                   start_day=start_day, end_day=end_day,
                   percentiles=percentiles,
                   timestamp=time_column,
                   extend_days=extend_search)


def _bq_convert_bool_sql():
    return """
    derived_number AS (
        SELECT
            event_day,
            event_hour,
            CASE event_column
            WHEN true  THEN 1
            WHEN false THEN 0
            END AS number_column
        FROM derived_base
        )
    """


def _bq_convert_time_sql():
    return """
    derived_number AS (
        SELECT
            event_day,
            event_hour,
            UNIX_SECONDS(event_column) AS number_column
        FROM derived_base
        )
    """


def _bq_convert_number_sql():
    return """
    derived_number AS (
        SELECT
            event_day,
            event_hour,
            event_column AS number_column
        FROM derived_base
        )
    """


def bq_make_convert_string_sql(slice_size, hash_size):
    def convert_string_sql():
        if slice_size >= 0:
            substr_parameters = "event_column,0,{no_characters}".format(no_characters=slice_size)
        else:
            substr_parameters = "reverse(event_column),0,{no_characters}".format(no_characters=(-1*slice_size))

        return """
        string_substr AS (
            SELECT
                event_day,
                event_hour,
                SUBSTR({parameters}) AS string_column
            FROM derived_base
            ),

        derived_number AS (
            SELECT
                event_day,
                event_hour,
                CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5(string_column)),0,{no_characters})) AS INT64) AS number_column
            FROM string_substr
            )
        """.format(parameters=substr_parameters, no_characters=hash_size)
    return convert_string_sql


def _bq_convert_hash_sql():
    return """
    derived_number AS (
        SELECT
            event_day,
            event_hour,
            STRTOL(SUBSTRING(event_column,0,15), 16) AS number_column
        FROM derived_base
        WHERE regexp_count(event_column, '[ABCDEF0123456789]') = length(event_column)
        )
    """


def _bq_normalize_sql():
    return """
    derived_stats AS (
        SELECT
            min(number_column) AS min_value,
            max(number_column) AS max_value,
            max(number_column) - min(number_column) AS range_value,
            avg(number_column) AS mean_value
        FROM derived_number
        WHERE number_column >= 0
        ),

    derived_norm AS (
        SELECT
            event_day,
            event_hour,
            number_column as raw_value,
            CASE
            WHEN (SELECT range_value from derived_stats) = 0
                THEN 0.1
            WHEN (CAST(number_column AS float64) - (SELECT min_value from derived_stats)) = 0
                THEN 0.000000000001
            ELSE
                (CAST(number_column AS float64) - (SELECT min_value from derived_stats))
                / (SELECT range_value from derived_stats)
            END AS normed_value
        FROM derived_number
        WHERE number_column >= 0
        )
    """


def _bq_read_normed_sql(sql_with):
    return """
    WITH
    {sql_with}

    SELECT
        event_day,
        event_hour,
        raw_value,
        normed_value
    FROM derived_norm
    """.format(sql_with=sql_with)


def bq_make_read_normed_percentiles_sql(number_percentiles):
    def read_normed_percentiles_sql(sql_with):
        queries = []
        idx = 0
        for tile in np.linspace(0.0, 1.0, num=number_percentiles+1):
            queries.append("""
                SELECT
                {tile:.2f} AS ptile,
                CASE
                WHEN ptiles[OFFSET({index})] is NULL THEN -1.0
                ELSE ptiles[OFFSET({index})]
                END as ptile_value
                FROM derived_percentiles
                """.format(tile=tile, index=idx))
            idx += 1
        sql_ptile = ' UNION ALL '.join(queries)
        
        sql = """
        WITH
        {sql_with},
        derived_percentiles AS (
            SELECT
                APPROX_QUANTILES(normed_value, {percentiles}) AS ptiles
            FROM derived_norm
            )
     
        {sql_ptiles}
        """.format(sql_ptiles=sql_ptile, sql_with=sql_with, percentiles=number_percentiles)
        return sql
    return read_normed_percentiles_sql


def _bq_read_basic_stats_sql(sql_with):
    return """
    WITH
    {sql_with}

    SELECT
        min(event_column) AS min_value,
        max(event_column) AS max_value,
        count(event_column) AS non_null_count,
        countif(event_column is NULL) AS null_count,
        count(distinct event_column) AS distinct_count
    FROM derived_base
    """.format(sql_with=sql_with)


def make_process_stats(name, read_stats, process_result, sort_columns, csv_file, csv_columns):
    def write_to_csv(results, csv_out):
        df = pd.concat(results)
        df = df.sort_values(sort_columns)
        df.to_csv(csv_out, header=False, index=False, columns=csv_columns)

    csv_name, csv_suffix = csv_file.split('.')


    def csv_for(table_name):
        return "{name}_{table}.{suffix}".format(name=csv_name, table=table_name, suffix=csv_suffix)


    def run_stats(table_column):
        table, column, start_day, end_day = table_column
        log_info("read {name} for {table}.{column}".format(name=name, table=table, column=column))
        df = read_stats(table, column, start_day, end_day)
        df['table_name'] = table
        df['column_name'] = column
        df = process_result(df)
        return df


    def group_the(tables):
        current_table = ''
        table_group = []
        tables_grouped = []

        for table, column, start_day, end_day in tables:
            if not current_table:
                current_table = table
            if current_table != table:
                tables_grouped.append(table_group)
                current_table = table
                table_group = []
            table_group.append((table, column, start_day, end_day))
        tables_grouped.append(table_group)

        return tables_grouped


    def process_stats(tables):
        tables_grouped = group_the(tables)

        from multiprocessing.pool import ThreadPool
        for table_group in tables_grouped:
            pool = ThreadPool(processes=16)
            results = pool.map(run_stats, table_group)
            pool.close()
            pool.join()
            table_name = table_group[0][0]
            write_to_csv(results, csv_for(table_name))
 
    return process_stats


def read_basic_stats(tables, column_basic_stats, csv_out):
    process_stats = make_process_stats(
                                'basic stats',
                                column_basic_stats,
                                lambda df: df, 
                                ['table_name', 'column_name'],
                                csv_out,
                                ['table_name', 'column_name',
                                'min_value', 'max_value',
                                'non_null_count', 'null_count',
                                'distinct_count'])
    process_stats(tables)


def read_percentiles(tables, column_percentiles, csv_out):
    def format_ptiles(df):
        df['ptile'] = df['ptile'].apply(lambda x: '{0:.2f}'.format(x))
        df['ptile_value'] = df['ptile_value'].apply(lambda x: '{0:.12f}'.format(x))
        return df

    process_stats = make_process_stats(
                                'percentiles',
                                column_percentiles,
                                format_ptiles,
                                ['table_name', 'column_name', 'ptile'],
                                csv_out,
                                ['table_name', 'column_name', 'ptile', 'ptile_value'])
    process_stats(tables)


def main(options):
    if options['--rs']:
        inject = rs_configure(options)
    elif options['--bq']:
        inject = bq_configure(options)

    if options['--pctl']:
        db_read_percentiles = inject['read_column_percentiles']
        csv_ptiles = inject['csv_ptiles']
        gen_tables = inject['gen_tables']
        read_percentiles(gen_tables, db_read_percentiles, csv_ptiles)
    elif options['--quick']:
        db_read_basic_stats = inject['read_column_basic_stats']
        csv_basic = inject['csv_basic']
        gen_tables = inject['gen_tables']
        read_basic_stats(gen_tables, db_read_basic_stats, csv_basic)


_usage="""
Calculate column stats

Usage:
  db_dist (--rs | --bq) (--pctl | --quick) PROJECT

Arguments:
  PROJECT    name of the project

Options:
  -h --help   show this
  --rs        use Redshift
  --bq        use Bigquery
  --pctl      calculate percentiles
  --quick     calculate min,max,distinct,..
"""

from docopt import docopt


if __name__ == '__main__':
    options = docopt(_usage)
    main(options)
