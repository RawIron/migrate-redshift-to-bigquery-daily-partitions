'''
Count the rows in a time range or on the whole table.
Tables and time range are read from a CSV file.
Outputs the results to a CSV file.
'''

from google.cloud import bigquery
import uuid
import pandas as pd

import bq_lib as bq
from config import config, load_config
from lib import make_gen_csv, log_info
import rs


#
# --> RedShift
#

def rs_configure(options):
    project = options['PROJECT']
    settings = config[project]['rs']
    schema = settings['schema']
    time_column = 'timestamp'

    aws_json = '../../etc/{proj}/aws.json'.format(proj=project)
    aws_config = load_config(aws_json)
    engine = rs.connect(settings['game_title'], aws_config)

    if options['--in']:
        csv_tables = options['--in']
    else:
        csv_tables = "rs_{project}_minmax_day.csv".format(project=project)

    if options['--out']:
        csv_count = options['--out']
    else:
        csv_count = "rs_{project}_table_daily_rows.csv".format(project=project)

    rs_query = rs.make_run(engine, schema)
    count_rows = rs_make_count_rows_daily(time_column)
    read_count = rs_make_count_daily(rs_query, count_rows)
    gen_tables = make_gen_csv(csv_tables)

    inject = {'ignore': settings['ignore_table'],
              'gen_tables': gen_tables,
              'read_count': read_count,
              'csv_count': csv_count,
             }
    return inject


def rs_make_count_rows_daily(time_column):
    def count_rows_daily(table, start_day, end_day):
        return """
        SELECT
            date({timestamp}) as day_part,
            count(*) as total_rows
        FROM {tablename}
        WHERE date({timestamp}) between '{start_day}' and '{end_day}'
        GROUP by day_part
        ORDER by day_part
        """.format(tablename=table,
                timestamp=time_column,
                start_day=start_day, end_day=end_day)
    return count_rows_daily


def rs_make_count_daily(rs_query, count_rows_daily):
    def count_daily(table, start_day, end_day):
        return rs_query(count_rows_daily(table, start_day, end_day))
    return count_daily


#
# --> BigQuery
#
def bq_configure_daily(options):
    project = options['PROJECT']
    settings = config[project]['bq']
    # project:dataset.tablename
    schema = ':'.join([settings['project'], settings['dataset']])
    time_column = 'timestamp'

    gcp_cfg = '../../etc/{proj}/gcp.json'.format(proj=project)
    gc_client = bigquery.Client.from_service_account_json(gcp_cfg)
    dataset = gc_client.dataset(settings['dataset'])

    if options['--in']:
        csv_tables = options['--in']
    else:
        csv_tables = "bq_{project}_minmax_day.csv".format(project=project)

    if options['--out']:
        csv_count = options['--out']
    else:
        csv_count = "bq_{project}_table_daily_rows.csv".format(project=project)


    count_rows = bq_make_count_rows_daily(time_column)
    read_count = bq_make_count_daily(gc_client, schema, count_rows)
    gen_tables = make_gen_csv(csv_tables)

    inject = {'csv_count': csv_count,
              'gen_tables': gen_tables,
              'read_count': read_count,
              'ignore': settings['ignore_table'],
             }
    return inject


def bq_configure_whole(options):
    project = options['PROJECT']
    settings = config[project]['bq']
    # project:dataset.tablename
    schema = ':'.join([settings['project'], settings['dataset']])

    gcp_cfg = '../../etc/{proj}/gcp.json'.format(proj=project)
    gc_client = bigquery.Client.from_service_account_json(gcp_cfg)
    dataset = gc_client.dataset(settings['dataset'])

    csv_tables = "bq_{project}_minmax_day.csv".format(project=project)
    csv_count = "bq_{project}_table_rows.csv".format(project=project)

    read_count = bq_make_whole_count(gc_client, schema)
    gen_tables = make_gen_csv(csv_tables)

    inject = {'csv_count': csv_count,
              'gen_tables': gen_tables,
              'read_count': read_count,
              'ignore': settings['ignore_table'],
             }
    return inject


def bq_configure(options):
    if options['--daily']:
        return bq_configure_daily(options)
    elif options['--whole']:
        return bq_configure_whole(options)
    else:
        return bq_configure_daily(options)


def bq_run_query(client, sql):
    query_job = client.run_async_query(str(uuid.uuid4()), sql)
    query_job.begin()
    query_job.result()

    destination_table = query_job.destination
    destination_table.reload()
    for row in destination_table.fetch_data():
        yield row


def bq_make_count_rows_daily(time_column):
    def count_rows_daily(table, start_day, end_day):
        return """
        SELECT
          DATE({timestamp}) AS day_part,
          COUNT({timestamp}) AS total_rows
        FROM
          [{table_id}]
        WHERE
          DATE({timestamp}) BETWEEN '{start_day}'
                                AND '{end_day}'
        GROUP BY
          day_part
        ORDER BY
          day_part
        """.format(table_id=table,
                timestamp=time_column,
                start_day=start_day, end_day=end_day)
    return count_rows_daily


def bq_make_count_daily(client, table_pre, count_rows_daily):
    def count_daily(table_id, start_day, end_day):
        table = '.'.join([table_pre, table_id])
        return bq_run_query(client, count_rows_daily(table, start_day, end_day))
    return count_daily


def _bq_count_whole_rows(table, column):
    return """
    SELECT
      COUNT({count_column}) AS total_rows
    FROM
      [{table_id}]
    """.format(table_id=table, count_column=column)


def bq_make_whole_count(client, table_prefix):
    def count_whole(table_name, column):
        table = '.'.join([table_prefix, table_name])
        return bq_run_query(client, _bq_count_whole_rows(table, column))
    return count_whole


#
# FUNCTIONALITY
#
def make_filter_tables(ignore):
    log_info("skip tables {ignore}".format(ignore=','.join(ignore)))
    filter_ignore = lambda tables: (table for table in tables if table not in ignore)
    return filter_ignore


# differences on TABLE
# tables do not have a _timestamp_ column
# list differences on the whole table as a first test

def make_count_whole(count_rows):
    def count_whole(table_info):
        table, column = table_info
        log_info("read row count for {table}".format(table=table))
        result = count_rows(table, column)
        for r in result:
            yield {'tablename': table, 'total_rows': r[0]}
    return count_whole


def count_rows_whole(tables, read_count, csv_out):
    '''
    tables
    |> read_count
    |> flatten
    |> to_dataframe
    |> to_csv
    '''
    results = (read_count(table) for table in tables)
    part_tables = (item for result in results for item in result)
    df = pd.DataFrame(part_tables)
    df.to_csv(csv_out, header=False, index=False,
              columns=['tablename', 'total_rows'])


# differences in DAY ranges
# tables have a _timestamp_ column

def make_count_daily(count_rows_daily):
    def count_daily(table_info):
        (table, start_day, end_day) = table_info
        log_info("read row count per day for {table}".format(table=table))
        result = count_rows_daily(table, start_day, end_day)
        for r in result:
            yield {'tablename': table, 'on_day': r[0], 'total_rows': r[1]}
    return count_daily


def count_rows_daily(tables, read_daily_count, csv_out):
    def read_from(generators):
        from multiprocessing.pool import ThreadPool
        pool = ThreadPool(processes=8)
        results = pool.map(lambda result: [r for r in result], generators)
        pool.close()
        pool.join()
        return results
    '''
    tables
    |> read_daily_count
    |> flatten
    |> to_dataframe
    |> to_csv
    '''
    result_generators = (read_daily_count(table) for table in tables)
    results = read_from(result_generators)
    # flatten results
    part_tables = (item for result in results for item in result)
    df = pd.DataFrame(part_tables)
    df.to_csv(csv_out, header=False, index=False,
              columns=['tablename', 'on_day', 'total_rows'])


def main(options):
    end_day = options['END_DAY']

    if options['--rs']:
        inject = rs_configure(options)
    elif options['--bq']:
        inject = bq_configure(options)

    if options['--daily']:
        ignore = inject['ignore']
        gen_tables = inject['gen_tables']
        gen_tables = ((table, start_day, end_day)
                        for table, start_day, _ in gen_tables
                        if table not in ignore and start_day)
        csv_count = inject['csv_count']
        f_db_daily = inject['read_count']
        f_count = make_count_daily(f_db_daily)
        count_rows = count_rows_daily

    if options['--whole']:
        gen_tables = inject['gen_tables']
        csv_count = inject['csv_count']
        f_db_count = inject['read_count']
        f_count = make_count_whole(f_db_count)
        count_rows = count_rows_whole

    count_rows(gen_tables, f_count, csv_count)


# Options probably must start with a unique letter
# --rsload and --rsunload does not work
_usage="""
Perform row count
Create csv with tablename,on_day,row_count

Usage:
  db_count (--rs | --bq) (--daily [--column=<c>] | --whole) [--in=<i>] [--out=<o>] PROJECT END_DAY

Arguments:
  PROJECT    name of the project
  END_DAY    upper bound in YYYYMMDD for row count

Options:
  -h --help     show this
  --rs          use Redshift
  --bq          use Bigquery
  --daily       only tables with time-column (events, facts)
  --column=<c>  name of time-column
  --whole       only tables with no time-column (dimensions)
  --in=<i>      read tables from this file
  --out=<o>     write row counts to this file
"""

from docopt import docopt


if __name__ == '__main__':
    options = docopt(_usage)
    main(options)
