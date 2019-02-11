import csv
import pandas as pd


def log_info(msg):
    print(msg)


def pp(df):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        pd.set_option('display.expand_frame_repr', False)
        print(df)


def make_gen_csv(csv_file):
    def gen_tuple():
        with open(csv_file, 'r') as f:
            for t in  csv.reader(f, delimiter=',', quotechar='"'):
                yield t
    return gen_tuple()


def make_is_not(f_filter):
    def is_not(table):
        return not f_filter(table)
    return is_not


def make_is_in(table_names):
    def is_in(table):
        if table.name in table_names: return True
        else: return False
    return is_in


def make_is_not_in(table_names):
    def is_not_in(table):
        if table.name not in table_names: return True
        else: return False
    return is_not_in


def make_filter_tables(f_filter):
    def apply_filter(tables):
        return [table for table in tables
                if f_filter(table)]
    return apply_filter
