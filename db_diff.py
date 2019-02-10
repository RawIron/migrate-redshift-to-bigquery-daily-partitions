'''
Diff row counts from Redshift and BigQuery
'''

import pandas as pd
import numpy as np

from config import config
from lib import log_info, pp


#
# --> RedShift
#
def rs_configure(options):
    project = options['PROJECT']
    settings = config[project]['rs']

    csv_count = "rs_{project}_table_daily_rows.csv".format(project=project)

    inject = {'csv_count': csv_count}
    return inject


#
# --> BigQuery
#
def bq_configure(options):
    project = options['PROJECT']
    settings = config[project]['bq']

    csv_count = "bq_{project}_table_rows.csv".format(project=project)
    csv_count = "bq_{project}_table_daily_rows.csv".format(project=project)

    inject = {'csv_count': csv_count}
    return inject


#
# FUNCTIONALITY
#
def make_filter_tables(ignore):
    log_info("skip tables {ignore}".format(ignore=','.join(ignore)))
    filter_ignore = lambda tables: (table for table in tables if table not in ignore)
    return filter_ignore


def validate_absolute(df_cmp, threshold=0):
    return df_cmp[abs(df_cmp.absolute_diff) > threshold]


def validate_relative(df_cmp, threshold=0.0):
    return df_cmp[abs(df_cmp.relative_diff) >= threshold]


def validate_summary(df_cmp):
    return {'bq_total': df_cmp.total_rows_bq.sum(),
            'diff_total': df_cmp.absolute_diff.sum(),
            'diff_relative': (df_cmp.absolute_diff.sum() * 100.0) / df_cmp.total_rows_rs.sum()}


def validate_table_relative(df_cmp, threshold=0.01):
    return df_cmp[df_cmp.relative_table_diff < -threshold]


def make_print_csv_rerun(csv_out):
    def print_csv_rerun(df):
        df = df[df['is_part'] == 1]
        df.to_csv(csv_out, header=False, index=True, columns=[])
    return print_csv_rerun


def make_print_csv_all(csv_out):
    def print_csv_all(df):
        df.to_csv(csv_out, header=False, index=True,
                  columns=['no_days',
                           'total_rows_rs', 'total_table_rs',
                           'total_rows_bq', 'total_table_bq',
                           'absolute_diff', 'relative_diff', 'relative_table_diff'])
    return print_csv_all


def _load_bq_partition(csv_partition, ignore):
    df = pd.read_csv(csv_partition, header=None,
                     names=['tablename', 'on_day'])
    df = df.set_index(['tablename', 'on_day'])
    df['is_part'] = 1
    return df


def _load_bq_daily(bq_csv, ignore):
    df = pd.read_csv(bq_csv, header=None,
            names=['tablename', 'on_day', 'total_rows'])
    df = df[~df.tablename.isin(ignore)]
    df['on_day'] = df['on_day'].str.replace('-', '')
    df['on_day'] = pd.to_numeric(df['on_day'])
    df = df.set_index(['tablename', 'on_day'])
    return df


def _load_bq(bq_csv, ignore):
    df_bq = _load_bq_daily(bq_csv, ignore)
    df_bq_part = _load_bq_partition('bq_bora_table_partitions.csv', ignore)
    df_bq = df_bq.join(df_bq_part)
    return df_bq


def _load_rs(rs_csv, ignore):
    df = pd.read_csv(rs_csv, header=None,
            names=['tablename', 'on_day', 'total_rows'])
    df = df[~df.tablename.isin(ignore)]
    df['on_day'] = df['on_day'].str.replace('-', '')
    df['on_day'] = pd.to_numeric(df['on_day'])
    df = df.set_index(['tablename', 'on_day'])
    return df


def _calc_diff(df_cmp):
    df_cmp['absolute_diff'] = df_cmp.total_rows_bq - df_cmp.total_rows_rs
    df_cmp['relative_diff'] = (df_cmp.absolute_diff *100.0) \
                               / df_cmp.total_rows_rs

    groups = df_cmp.groupby(axis=0, level=0)
    table_total = groups['total_rows_rs'].aggregate(np.sum)
    df_cmp['total_table_rs'] = df_cmp.join(table_total,
                                           rsuffix='_table')['total_rows_rs_table']

    table_total = groups['total_rows_bq'].aggregate(np.sum)
    df_cmp['total_table_bq'] = df_cmp.join(table_total,
                                           rsuffix='_table')['total_rows_bq_table']

    table_parts = groups['total_rows_rs'].count()
    df_cmp['no_days'] = df_cmp.join(table_parts,
                                     rsuffix='_part')['total_rows_rs_part']

    df_cmp['relative_table_diff'] = (df_cmp.absolute_diff *100.0) \
                                     / df_cmp.total_table_rs
    
    return df_cmp


def verify(rs_csv, bq_csv, validator=validate_summary, printer=pp, ignore=[]):
    '''
    load_sources
    |> join_sources
    |> calc_diff
    |> filter_diff
    |> print_diff
    '''
    df_rs = _load_rs(rs_csv, ignore)
    df_bq = _load_bq(bq_csv, ignore)

    df_cmp = df_rs.join(df_bq, lsuffix='_rs', rsuffix='_bq')
    df_cmp = _calc_diff(df_cmp)

    if validator == validate_summary:
        pp(validator(df_cmp))
    else:
        printer(validator(df_cmp))


def main(options):
    end_day = options['END_DAY']
    project = options['PROJECT']

    rs_inject = rs_configure(options)
    bq_inject = bq_configure(options)

    printer_opt = options['--printer']
    printer = None
    if printer_opt == 'csv_all':
        csv_out = "verify_{project}.csv".format(project=project)
        printer = make_print_csv_all(csv_out)
    elif printer_opt == 'csv_rerun':
        csv_out = "rerun_partitions_{project}.csv".format(project=project)
        printer = make_print_csv_rerun(csv_out)
    elif printer_opt == 'pp':
        printer = pp

    validator_opt = options['--validator']
    validator = None
    if validator_opt == 'relative':
        validator = validate_relative
    elif validator_opt == 'absolute':
        validator = validate_absolute
    elif validator_opt == 'summary':
        validator = validate_summary

    ignore = ['storm_warn', 'weather_adjust']
    verify(rs_inject['csv_count'], bq_inject['csv_count'],
           validator, printer, ignore)


# Options probably must start with a unique letter
# --rsload and --rsunload does not work
_usage="""
Compare the rs and bq row_count

Usage:
  db_diff [--printer=<p>] [--validator=<v>] PROJECT END_DAY

Arguments:
  PROJECT    name of the project
  END_DAY    upper bound in YYYYMMDD for compare

Options:
  -h --help         show this
  --printer=<p>     pick pp, csv_all, csv_rerun [default: pp]
  --validator=<v>   pick summary, absolute, relative [default: summary]
"""

from docopt import docopt


if __name__ == '__main__':
    options = docopt(_usage)
    main(options)
