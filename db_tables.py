'''
functions return a generator
functions could also return a list without any issues

generators are streams
the code does _not_ communicate the stream processing design well
'''

from google.cloud import bigquery
import pandas as pd
import uuid
from functools import reduce

import bq_lib as bq
from config import config, load_config
from lib import make_gen_csv, log_info, pp
import rs


#
# --> RedShift
#

def rs_configure_db_pandas(project, settings):
    '''
    sqlalchemy for pandas dataframe
    '''
    schema = settings['schema']
    aws_json = settings['aws_json']
    aws_config = load_config(aws_json)
    engine = rs.connect(project, aws_config)
    conn = engine.connect()
    conn.execute("SET search_path TO {schema}".format(schema=schema))
    return conn


def rs_configure_db_core(project, settings):
    '''
    run sql with sqlalchemy core
    '''
    schema = settings['schema']
    aws_json = settings['aws_json']
    aws_config = load_config(aws_json)
    engine = rs.connect(project, aws_config)
    rs_query = rs.make_run(engine, schema)
    return rs_query


def rs_configure(options):
    project = options['PROJECT']
    settings = config[project]['rs']
    schema = settings['schema']

    rs_query = rs_configure_db_core(settings['title'], settings)

    csv_whole = "rs_{project}_table.csv".format(project=project)
    csv_tables = "rs_{project}_minmax_day.csv".format(project=project)
    csv_columns = "rs_{project}_columns_minmax_day.csv".format(project=project)
    gen_tables = make_gen_csv(csv_tables)

    list_tables = rs_make_read_all_tables(rs_query, schema)

    read_columns = rs_make_read_columns(rs_query, schema)
    put_columns = rs_make_columns(read_columns)
    read_notnull_columns = rs_make_read_notnull_columns(rs_query, schema)
    put_required = rs_make_put_required(read_notnull_columns, read_columns)
    read_min_day = rs_make_read_min_day(rs_query)
    put_min_day = rs_make_min_day(read_min_day)
    read_max_day = rs_make_read_max_day(rs_query)
    put_max_day = rs_make_max_day(read_max_day)

    filter_daily = rs_make_filter_daily(settings['time_columns'])
    filter_whole = rs_make_filter_whole(settings['time_columns'])
    filter_ignore = make_filter_ignore(settings['ignore_table'])
    filter_only = make_filter_only([])
    filter_ignore_column = make_filter_ignore_column(settings['ignore_column'])
    filter_only_column = make_filter_only_column(settings['only_column'])

    inject = {
              'put_min_day': put_min_day,
              'put_max_day': put_max_day,
              'put_columns': put_columns,
              'put_required': put_required,
              'filter_ignore': filter_ignore,
              'filter_only': filter_only,
              'filter_daily': filter_daily,
              'filter_whole': filter_whole,
              'filter_ignore_column': filter_ignore_column,
              'filter_only_column': filter_only_column,
              'read_tables': list_tables,
              'gen_tables': gen_tables,
              'csv_tables': csv_tables,
              'csv_whole_tables': csv_whole,
              'csv_columns': csv_columns,
             }
    return inject


def _rs_read_all_tables_sql(schema):
    return """
       SELECT tablename
       FROM pg_tables
       WHERE schemaname = '{schema}'
    """.format(schema=schema)


def rs_make_read_all_tables(rs_query, schema):
    class Table(dict):
            __getattr__, __setattr__ = dict.get, dict.__setitem__

    def read_all_tables():
        result = rs_query(_rs_read_all_tables_sql(schema))
        for r in result:
            yield Table({'name': r[0]})
    return read_all_tables()


def rs_make_filter_daily(time_columns):
    log_info("use time-columns [{time}]".format(time=','.join(time_columns)))
    def filter_daily(_, acc):
        if any(c in time_columns for c in acc['columns']):
            return True, acc
        else:
            return False, acc
    return filter_daily


def rs_make_filter_whole(time_columns):
    def filter_whole(_, acc):
        if any(c in time_columns for c in acc['columns']):
            return False, acc
        else:
            return True, acc
    return filter_whole


def _rs_read_notnull_columns_sql(schema_name, table_name):
    return """
    SELECT
        column_name
    FROM information_schema.columns 
    WHERE table_schema = '{table_schema}'
    AND table_name = '{table_name}'
    AND is_nullable = 'NO'
    """.format(table_schema=schema_name, table_name=table_name)


def rs_make_read_notnull_columns(rs_query, schema):
    def read_notnull_columns(table):
        result = rs_query(_rs_read_notnull_columns_sql(schema, table))
        for r in result:
            yield r[0]
    return read_notnull_columns


def rs_make_put_required(read_notnull_columns, read_columns):
    def put_required(table, acc):
        required_or_any = next(read_notnull_columns(table.name), next(read_columns(table.name)))
        acc['column_name'] = [required_or_any]
        return True, acc
    return put_required


def _rs_read_columns_sql(schema_name, table_name):
    return """
    SELECT
        column_name
    FROM information_schema.columns 
    WHERE table_schema = '{table_schema}'
    AND table_name = '{table_name}'
    """.format(table_schema=schema_name, table_name=table_name)


def rs_make_read_columns(rs_query, schema):
    def read_columns(table):
        result = rs_query(_rs_read_columns_sql(schema, table))
        for r in result:
            yield r[0]
    return read_columns


def rs_make_columns(read_columns):
    def put_columns(table, acc):
        acc['columns'] = list(read_columns(table.name))
        return True, acc
    return put_columns


def _rs_read_min_day_sql(table):
    return """
    SELECT
        MIN(date(timestamp)) as min_day
    FROM {tablename}
    WHERE date(timestamp) < current_date
    """.format(tablename=table)


def rs_make_read_min_day(rs_query):
    def read_min_day(table):
        result = rs_query(_rs_read_min_day_sql(table))
        for r in result:
            yield r[0]
    return read_min_day


def rs_make_min_day(read_min_day):
    def put_min_day(table, acc):
        acc['min_day'] = list(read_min_day(table.name))
        return True, acc
    return put_min_day


def _rs_read_max_day_sql(table):
    return """
    SELECT
        MAX(date(timestamp)) as max_day
    FROM {tablename}
    WHERE date(timestamp) < current_date
    """.format(tablename=table)


def rs_make_read_max_day(rs_query):
    def read_max_day(table):
        result = rs_query(_rs_read_max_day_sql(table))
        for r in result:
            yield r[0]
    return read_max_day


def rs_make_max_day(read_max_day):
    def put_max_day(table, acc):
        acc['max_day'] = list(read_max_day(table.name))
        return True, acc
    return put_max_day


#
# --> BigQuery
#

def bq_configure(options):
    project = options['PROJECT']
    settings = config[project]['bq']
    # project:dataset.tablename
    schema = ':'.join([settings['project'], settings['dataset']])

    gcp_cfg = '../../etc/{proj}/gcp.json'.format(proj=project)
    gc_client = bigquery.Client.from_service_account_json(gcp_cfg)
    dataset = gc_client.dataset(settings['dataset'])

    csv_tables = "bq_{project}_minmax_day.csv".format(project=project)
    gen_tables = make_gen_csv(csv_tables)
    csv_columns = "bq_{project}_columns_minmax_day.csv".format(project=project)
    csv_partition = "bq_{project}_table_partitions.csv".format(project=project)

    read_tables = bq.make_list_tables(dataset, lambda x: x)()

    read_columns = bq_make_read_columns()
    put_columns = bq_make_columns(read_columns)
    read_min_day = bq_make_read_min_day(gc_client, settings['dataset'])
    put_min_day = bq_make_min_day(read_min_day)
    read_max_day = bq_make_read_max_day(gc_client, settings['dataset'])
    put_max_day = bq_make_max_day(read_max_day)
    read_partitions = bq_make_read_partitions(gc_client, settings['dataset'])
    put_partitions = bq_make_partitions(read_partitions)

    filter_daily = bq_make_filter_daily(settings['time_columns'])
    filter_whole = bq_make_filter_whole(settings['time_columns'])
    filter_ignore = make_filter_ignore(settings['ignore_table'])
    filter_only = make_filter_only(settings['only_table'])
    filter_ignore_column = make_filter_ignore_column(settings['ignore_column'])
    filter_only_column = make_filter_only_column(settings['only_column'])

    inject = {
            'put_min_day': put_min_day,
            'put_max_day': put_max_day,
            'put_columns': put_columns,
            'put_required': bq_put_required,
            'put_partitions': put_partitions,
            'filter_ignore': filter_ignore,
            'filter_only': filter_only,
            'filter_daily': filter_daily,
            'filter_whole': filter_whole,
            'filter_ignore_column': filter_ignore_column,
            'filter_only_column': filter_only_column,
            'read_tables': read_tables,
            'gen_tables': gen_tables,
            'csv_tables': csv_tables,
            'csv_columns': csv_columns,
            'csv_partition': csv_partition,
            }
    return inject


def bq_run_query(client, sql):
    query_job = client.run_async_query(str(uuid.uuid4()), sql)
    query_job.begin()
    query_job.result()
    destination_table = query_job.destination
    destination_table.reload()
    for row in destination_table.fetch_data():
        yield row


def _bq_read_all_tables_sql(dataset_name):
    return """
    SELECT table_id 
    FROM [{schema}.__TABLES__]
    """.format(schema=dataset_name)


def bq_make_read_all_tables(client, dataset_name):
    def read_all_tables():
        return bq_run_query(client, _bq_read_all_tables_sql(dataset_name))
    return read_all_tables


def bq_make_filter_daily(time_columns):
    def filter_daily(table, acc):
        if bq.is_daily(table):
            return True, acc
        else:
            return False, acc
    return filter_daily


def bq_make_filter_whole(time_columns):
    def filter_whole(table, acc):
        if not bq.is_daily(table) and bq.has_schema(table):
            return True, acc
        else:
            return False, acc
    return filter_whole


def bq_make_read_columns():
    def read_columns(table):
        return (field.name for field in table.schema)
    return read_columns


def bq_make_columns(read_columns):
    def put_columns(table, acc):
        acc['columns'] = list(read_columns(table))
        return True, acc
    return put_columns


def _bq_read_min_day_sql(table):
    return """
    SELECT
        MIN(date(timestamp)) as min_day
    FROM [{tablename}]
    WHERE date(timestamp) < CURRENT_DATE()
    """.format(tablename=table)


def bq_make_read_min_day(client, dataset_name):
    def read_min_day(table):
        table = '.'.join([dataset_name, table])
        result = bq_run_query(client, _bq_read_min_day_sql(table))
        for r in result:
            yield r[0]
    return read_min_day


def bq_make_min_day(read_min_day):
    def put_min_day(table, acc):
        acc['min_day'] = list(read_min_day(table.name))
        return True, acc
    return put_min_day


def bq_make_read_max_day(client, dataset_name):
    def read_max_day_sql(table):
        return """
        SELECT
            MAX(date(timestamp)) as max_day
        FROM [{tablename}]
        WHERE date(timestamp) < CURRENT_DATE()
        """.format(tablename=table)

    def read_max_day(table):
        table = '.'.join([dataset_name, table])
        result = bq_run_query(client, read_max_day_sql(table))
        for r in result:
            yield r[0]
    return read_max_day


def bq_make_max_day(read_max_day):
    def put_max_day(table, acc):
        acc['max_day'] = list(read_max_day(table.name))
        return True, acc
    return put_max_day


def bq_put_required(table, acc):
    acc['column_name'] = [bq.get_required_or_any_field(table)]
    return True, acc


def _bq_read_partition_sql(table):
    return """
    SELECT
        partition_id
    FROM [{table}$__PARTITIONS_SUMMARY__]
    """.format(table=table)


def bq_make_read_partitions(client, dataset_name):
    def read_partitions(table):
        table = '.'.join([dataset_name, table])
        result = bq_run_query(client, _bq_read_partition_sql(table))
        for r in result:
            yield r[0]
    return read_partitions


def bq_make_partitions(read_partitions):
    def put_partitions(table, acc):
        acc['partitions'] = list(read_partitions(table.name))
        return True, acc
    return put_partitions


#
# FUNCTIONALITY
#

def make_filter_ignore(blacklist):
    log_info("skip tables [{blacklist}]".format(blacklist=','.join(blacklist)))
    def filter_ignore(table, acc):
        if table.name not in blacklist:
            return True, acc
        else:
            return False, acc
    return filter_ignore


def make_filter_only(whitelist):
    log_info("only tables [{whitelist}]".format(whitelist=','.join(whitelist)))
    def filter_only(table, acc):
        if whitelist and table.name in whitelist:
            return True, acc
        elif whitelist:
            return False, acc
        else:
            return True, acc
    return filter_only


def make_filter_only_column(whitelist):
    log_info("only columns [{whitelist}]".format(whitelist=','.join(whitelist)))
    def filter_only_column(_, acc):
        if whitelist:
            acc['columns'] = whitelist
        return True, acc
    return filter_only_column


def make_filter_ignore_column(blacklist):
    log_info("ignore columns [{blacklist}]".format(blacklist=','.join(blacklist)))
    def filter_ignore_column(_, acc):
        acc['columns'] = [col for col in acc['columns'] if col not in blacklist]
        return True, acc
    return filter_ignore_column


def process(tables, func_list):
    result = []
    for table in tables:
        acc = {'table_name': [table.name]}
        for f in func_list:
            ok, acc = f(table, acc)
            if not ok:
                log_info("skip {table}".format(table=table.name))
                acc = {}
                break
        if acc:
            log_info("processed {table}".format(table=table.name))
            result.append(acc)
    return result


def export_list(table_results, csv_out, csv_columns):
    frames = []
    for result in table_results:
        log_info("exporting {table}".format(table=result['table_name']))
        frame_columns = []
        for key,value in result.items():
            if key in csv_columns:
                tmp_df = pd.DataFrame({key: value})
                tmp_df['_key'] = 1
                frame_columns.append(tmp_df)
        frames.append(reduce(lambda x,y: pd.merge(x,y,on='_key'), frame_columns))

    if frames:
        df = reduce(lambda x,y: pd.concat([x,y]), frames)
        df = df.sort_values(csv_columns)
        df.to_csv(csv_out, header=False, index=False,
                  columns=csv_columns)


def main(options):
    end_day = options['END_DAY']

    if options['--rs']:
        inject = rs_configure(options)
    elif options['--bq']:
        inject = bq_configure(options)

    if options['--daily']:
        f_tables = inject['read_tables']
        func_list = [
                inject['filter_only'],
                inject['filter_ignore'],
                inject['put_columns'],
                inject['filter_only_column'],
                inject['filter_ignore_column'],
                inject['filter_daily'],
                inject['put_min_day'],
                inject['put_max_day'],
                ]

    elif options['--daily'] and options['--part']:
        gen_tables = inject['gen_tables']
        f_tables = (table for table, _, _ in gen_tables
                    if table not in ignore)
        func_list = [
                inject['filter_only'],
                inject['filter_ignore'],
                inject['put_columns'],
                inject['filter_daily'],
                inject['put_partitions']
                ]
        csv_out = inject['csv_partition']
        csv_columns = ['table_name', 'partitions']

    elif options['--whole']:
        f_tables = inject['read_tables']
        func_list = [
                inject['filter_only'],
                inject['filter_ignore'],
                inject['put_columns'],
                inject['filter_whole'],
                inject['put_required'],
                ]

    if options['--tables'] and options['--daily']:
        csv_out = inject['csv_tables']
        csv_columns = ['table_name', 'min_day', 'max_day']

    if options['--tables'] and options['--whole']:
        csv_out = inject['csv_whole_tables']
        csv_columns = ['table_name', 'column_name']

    if options['--columns'] and options['--daily']:
        csv_out = inject['csv_columns']
        csv_columns = ['table_name', 'columns', 'min_day', 'max_day']

    if options['--columns'] and options['--whole']:
        csv_out = inject['csv_whole_columns']
        csv_columns = ['table_name', 'columns']

    results = process(f_tables, func_list)
    export_list(results, csv_out, csv_columns)


# Options probably must start with a unique letter
# --rsload and --rsunload does not work
_usage="""
Export the requested table list into csv

Usage:
  db_tables (--rs | --bq) (--daily | --whole) (--tables | --columns | --part) PROJECT END_DAY

Arguments:
  PROJECT    name of the project
  END_DAY    upper bound in YYYYMMDD for an operation, for example --verify

Options:
  -h --help   show this
  --rs        use Redshift
  --bq        use Bigquery
  --daily     only tables with time-column (events, facts)
  --whole     only tables with no time-column (dimensions)
  --tables    create csv with tablename,min_day,max_day
  --columns   create csv with tablename,columnname,min_day,max_day
  --part      create csv with tablename,partition
"""

from docopt import docopt


if __name__ == '__main__':
    options = docopt(_usage)
    main(options)
