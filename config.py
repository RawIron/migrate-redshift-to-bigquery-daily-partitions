import json


config = {
    'prod-bora-bq': {
        'rs': {
            'title': 'bora',
            'aws_json': './etc/prod-bora-bq/aws.json',
            'db': 'ef4',
            'schema': 'bora',
            'time_columns': ['timestamp'],
            'ignore_table': [],
            'only_table': [],
            'ignore_column': ['insertid'],
            'only_column': [],
            },
        'bq': {
            'title': 'bora',
            'project': 'zephyrus-ef4-prod-bora',
            'dataset': 'bora',
            'time_columns': ['timestamp'],
            'ignore_table': [],
            'only_table': [],
            'ignore_column': [],
            'only_column': [],
            }
        },

    'prod-ostro-bq': {
        'rs': {
            'title': 'ostro',
            'aws_json': './etc/prod-ostro-bq/aws.json',
            'db': 'ef4',
            'schema': 'ostro',
            'time_columns': ['timestamp'],
            'ignore_table': [],
            'only_table': [],
            'ignore_column': ['insertid'],
            'only_column': [],
            },
        'bq': {
            'title': 'ostro',
            'project': 'zephyrus-ef4-prod-ostro',
            'dataset': 'ostro',
            'time_columns': ['timestamp'],
            'ignore_table': [],
            'only_table': [],
            'ignore_column': [],
            'only_column': [],
            }
        },

    'dev-ostro-bq': {
        'rs': {
            'title': 'dev-ostro',
            'aws_json': './etc/dev-ostro-bq/aws.json',
            'db': 'ef4',
            'schema': 'ostro',
            'time_columns': ['timestamp'],
            'ignore_table': [],
            'only_table': [],
            'ignore_column': ['insertid'],
            'only_column': [],
            },
        'bq': {
            'title': 'dev-ostro',
            'project': 'zephyrus-ef4-dev-ostro',
            'dataset': 'ostro',
            'time_columns': ['timestamp'],
            'ignore_table': [],
            'only_table': [],
            'ignore_column': [],
            'only_column': [],
            }
        }
    }


def load_config(json_file):
    with open(json_file) as json_config:
        config = json.load(json_config)
    return config
