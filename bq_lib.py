import uuid
import time

from lib import log_info


def _job_id():
    return str(uuid.uuid4())


def is_partitioned(table):
    if table.partitioning_type == 'DAY': return True
    else: return False


def is_daily(table):
    if 'timestamp' in [c.name for c in table.schema]: return True
    else: False


def has_schema(table):
    if table.schema: return True
    else: False


def get_required_or_any_field(table):
    '''
    BUG for any_field
    in case order of fields in a table is different on the 2 datasets used in a diff
    and count of NULL values vary in the 2 different fields
    diff will show a false-difference
    '''
    for field in table.schema:
        if field.mode == 'REQUIRED':
            return field.name
    return table.schema[0].name


def drop(tables):
    for table in tables:
        table.delete()


def copy_data(client, dest, source):
    '''
    only works when schema of source and destination are identical
    all data is inserted into _current_ day partition
    '''
    job = client.copy_table(_job_id(), dest, source)
    job.begin()
    retry_count = 100
    while retry_count > 0 and job.state != 'DONE':
        retry_count -= 1
        time.sleep(10)
        job.reload()


def make_insert_select(client):
    def insert_select(dest, source):
        '''
        use INSERT-SELECT to copy data from source to destination
        '''
        columns = ','.join([c.name for c in dest.schema])
        query = """
        SELECT {columns}
        FROM [{table}]
        """.format(table=source.select_id, columns=columns)

        job_data = {
            "jobReference": {
              "projectId": dest.project,
              "jobId": "{jobid}".format(jobid=_job_id())
            },
            "configuration": {
              "query": {
                 "query": "{query}".format(query=query),
                 "allowLargeResults": "True",
                 "destinationTable": {
                    "projectId": "{project}".format(project=dest.project),
                    "datasetId": "{dataset}".format(dataset=dest.dataset_name),
                    "tableId": "{table}".format(table=dest.insert_id)
                 }
              }
            }
        }

        job = client.job_from_resource(job_data)
        job.begin()
        return job.result()
    return insert_select


def make_make_insert_data(do_insert_select):
    '''
    create a factory method
    inject the factory method so functions can make_ dynamically
    '''
    def make_insert_data(source):
        def insert_partition_data(dest, source):
            for partition in source.list_partitions():
                source.select_id = ''.join([source.table_id, "$", partition])
                dest.insert_id = ''.join([dest.name, "$", partition])
                do_insert_select(dest, source)

        def insert_data(dest, source):
            source.select_id = source.table_id
            dest.insert_id = dest.name
            do_insert_select(dest, source)

        if is_partitioned(source):
            return insert_partition_data
        else:
            return insert_data
    return make_insert_data


def make_load(make_insert):
    def load(dest_tables, source_tables):
        for dest, source in zip(dest_tables, source_tables):
            log_info("copy from {source} to {dest}".format(source=source.name, dest=dest.name))
            insert_data = make_insert(source)
            insert_data(dest, source)
    return load


def make_copy_table(dataset, exclude_columns, rename):
    def copy_table(original):
        schema = [col for col in original.schema
                  if col.name not in exclude_columns]
        t = dataset.table(rename(original.name), schema)
        t.partitioning_type = 'DAY'
        t.create()
        return t
    return copy_table


def make_copy(f_copy):
    def copy(tables):
        return [f_copy(table) for table in tables]
    return copy


def make_list_tables(dataset, apply_filter):
    def list_tables():
        tables = list(dataset.list_tables())
        for table in tables:
            table.reload()
        return apply_filter(tables)
    return list_tables

