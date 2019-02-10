'''
copy BigQuery Tables to another project

the copy preserves all DAY partitions
processes the tables sequentially
'''
from google.cloud import bigquery
from bq_lib import *


def src_configure(settings):
    client = bigquery.Client.from_service_account_json(settings['gcp_cfg'])
    dataset = client.dataset(settings['dataset'])

    if settings['only_use']:
        f_filter = make_is_in(settings['only_use'])
    else:
        f_filter = make_is_not(make_is_in(settings['ignore']))
    table_filter = make_filter_tables(f_filter)
    list_tables = make_list_tables(dataset, table_filter)

    copy = lambda : True

    insert_select = make_insert_select(client)
    make_insert_data = make_make_insert_data(insert_select)
    load = make_load(make_insert_data)

    return {'list_tables': list_tables,
            'load': load,
            'copy': copy
           }


def dest_configure(settings):
    client = bigquery.Client.from_service_account_json(settings['gcp_cfg'])
    dataset = client.dataset(settings['dataset'])

    if settings['only_use']:
        f_filter = make_is_in(settings['only_use'])
    else:
        f_filter = make_is_not(make_is_in(settings['ignore']))
    table_filter = make_filter_tables(f_filter)
    list_tables = make_list_tables(dataset, table_filter)

    rename = lambda x: x
    copy_table = make_copy_table(dataset, [], rename)
    copy = make_copy(copy_table)

    insert_select = make_insert_select(client)
    make_insert_data = make_make_insert_data(insert_select)
    load = make_load(make_insert_data)

    return {'list_tables': list_tables,
            'load': load,
            'copy': copy
           }


def configure(options):
    config = {
            'dev-ostro': {
                'gcp_cfg': './etc/dev-ostro/gcp.json',
                'only_use': [],
                'ignore' : ['storm_warn', 'weather_extern',
                            'weather_hist'],
                'project': 'zephyrus-ef4-prod-ostro',
                'dataset': 'ostro',
                },
            'ostro': {
                'gcp_cfg': './etc/prod-ostro/gcp.json',
                'only_use': [],
                'ignore' : ['storm_warn', 'weather_extern',
                            'weather_hist'],
                'project': 'zephyrus-ef4-prod-ostro',
                'dataset': 'ostro',
                } 
            }

    game = options['SOURCE']
    src_config = src_configure(config[game])

    game = options['DESTINATION']
    dest_config = dest_configure(config[game])

    return src_config, dest_config


def main(options):
    src, dest = configure(options)

    # in Ruby or Elixir when the key of the dictionary is an Atom
    # src.list_tables()
    all_tables = src['list_tables']()

    if options['--copy']:
        dest_tables = dest['copy'](all_tables)
    elif options['--load']:
        dest_tables = dest['list_tables']()
        dest['load'](dest_tables, all_tables)
    elif options['--drop']:
        dest_tables = dest['list_tables']()
        drop(dest_tables)


# Options probably must start with a unique letter
# --reverse and --restore does not work
_usage="""
Copy BigQuery tables to another project.
Preserve all DAY partitions.

Usage:
  bq_copy_project (--copy | --load | --drop) SOURCE DESTINATION

Arguments:
  SOURCE      name of the project
  DESTINATION name of the project

Options:
  -h --help  show this
  --copy     create copies of all tables in DESTINATION
  --load     load the data into DESTINATION tables
  --drop     drop all tables in DESTINATION
"""

from docopt import docopt


if __name__ == '__main__':
    options = docopt(_usage)
    main(options)

