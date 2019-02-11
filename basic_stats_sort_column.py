import pandas as pd
import os

files = os.listdir()

for csv_file in files:
    df = pd.read_csv(csv_file, header=None,
            names=[
                'table', 'column',
                'min_value', 'max_value',
                'non_null_count', 'null_count',
                'distinct_count'])

    df = df.sort_values(['table', 'column'])

    csv_out = "{fname}.sorted".format(fname=csv_file)
    df.to_csv(csv_out, header=False, index=False,
            columns=[
                'table', 'column',
                'min_value', 'max_value',
                'non_null_count', 'null_count',
                'distinct_count'])
