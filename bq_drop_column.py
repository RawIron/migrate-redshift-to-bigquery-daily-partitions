'''
operations on BigQuery Tables
    drop a column
    make a backup copy or copy
    restore from a backup copy

the operations preserve all DAY partitions
processes the tables sequentially
'''
from google.cloud import bigquery
from bq_lib import *


def make_ends_with(suffix, separator='_'):
    def has_suffix(name):
        return name.split(separator)[-1:][0] in suffix
    return has_suffix


def make_any_copy(suffix, separator='_'):
    def any_copy(names):
        return [separator.join([n,s]) for n in names for s in suffix]
    return any_copy


def make_reverse_rename(suffix, separator='_'):
    def reverse_rename(name):
        parts = name.split(separator)
        if parts[-1:] == [suffix]: return separator.join(parts[:-1])
        else: return name
    return reverse_rename


def build_copy(dataset, exclude_columns, separator, appendix):
    rename = lambda x: separator.join([x, appendix])
    copy_table = make_copy_table(dataset, exclude_columns, rename)
    copy = make_copy(copy_table)

    reverse_rename = make_reverse_rename(appendix, separator)
    reverse_copy_table = make_copy_table(dataset, exclude_columns, reverse_rename)
    reverse_copy = make_copy(reverse_copy_table)

    return copy, reverse_copy


def configure(options):
    separator = '_'
    reserved_suffix = ['copy', 'backup']
    is_copy = make_ends_with(['copy'], separator)
    is_any_copy = make_ends_with(reserved_suffix, separator)
    is_backup = make_ends_with(['backup'], separator)
    any_copy = make_any_copy(reserved_suffix, separator)

    game = options['PROJECT']

    config = {
        'bora': {
            'project': 'zephyrus-ef4-prod-bora',
            'dataset': 'bora',
            'ignore' : [
                'temp_adjust', 'metric_names',
                'metric_fail'],
            },
        'ostro': {
            'project': 'zephyrus-ef4-prod-ostro',
            'dataset': 'ostro',
            'ignore' : [],
            },
        'dev-ostro': {
            'project': 'zephyrus-ef4-dev-ostro',
            'dataset': 'ostro',
            'ignore' : [],
            } 
       }

    settings = cnfig[game]
    gcp_cfg = './etc/{proj}/gcp.json'.format(proj=game)

    client = bigquery.Client.from_service_account_json(gcp_cfg)
    dataset = client.dataset(settings['dataset'])

    only_use = []
    only_use_any = only_use + any_copy(only_use)
    ignore = settings['ignore']
    ignore_any = ignore + any_copy(ignore)


    if only_use:
        f_filter = make_is_in(only_use_any)
    else:
        f_filter = make_is_not(make_is_in(ignore_any))
    table_filter = make_filter_tables(f_filter)
    list_tables = make_list_tables(dataset, table_filter)

    copy, reverse_copy = build_copy(dataset, ['insertid'], '_', 'copy')
    backup, reverse_backup = build_copy(dataset, [], '_', 'backup')

    insert_select = make_insert_select(client)
    make_insert_data = make_make_insert_data(insert_select)
    load = make_load(make_insert_data)

    return {'is_copy': is_copy, 'is_any_copy': is_any_copy,
            'is_backup': is_backup,
            'list_tables': list_tables, 'load': load,
            'copy': copy, 'reverse_copy': reverse_copy,
            'backup': backup, 'reverse_backup': reverse_backup,
           }


def main(options):
    inject = configure(options)

    list_tables = inject['list_tables']
    load = inject['load']
    copy = inject['copy']
    reverse_copy = inject['reverse_copy']
    backup = inject['backup']
    reverse_backup = inject['reverse_backup']

    is_copy = inject['is_copy']
    is_any_copy = inject['is_any_copy']
    is_backup = inject['is_backup']

    all_tables = list_tables()

    if options['--backup']:
        tables = [table for table in all_tables
                  if not is_any_copy(table.name)]
        tables_copy = backup(tables)
        load(tables_copy, tables)
    elif options['--undo']:
        tables = [table for table in all_tables
                  if is_backup(table.name)]
        tables_copy = reverse_backup(tables)
        load(tables_copy, tables)
    elif options['--copy']:
        tables = [table for table in all_tables
                  if not is_any_copy(table.name)]
        tables_copy = copy(tables)
        load(tables_copy, tables)
    elif options['--reverse']:
        tables = [table for table in all_tables
                  if is_copy(table.name)]
        tables_copy = reverse_copy(tables)
        load(tables_copy, tables)
    elif options['--drop']:
        tables = [table for table in all_tables
                  if not is_any_copy(table.name)]
        drop(tables)
    elif options['--clean']:
        tables = [table for table in all_tables
                  if is_any_copy(table.name)]
        drop(tables)


# Options probably must start with a unique letter
# --reverse and --restore does not work
_usage="""
Perform the requested operation on BigQuery tables

Usage:
  bq_drop_column (--backup | --undo | --copy | --drop | --reverse | --clean) PROJECT

Arguments:
  PROJECT    name of the project

Options:
  -h --help  show this
  --backup   create backup copies of all tables
  --undo     put the backup copies in place
  --copy     create copies of all tables
  --drop     drop all tables not a copy or a backup
  --reverse  put the copies in place
  --clean    drop all backup copies and copies
"""

from docopt import docopt


if __name__ == '__main__':
    options = docopt(_usage)
    main(options)

