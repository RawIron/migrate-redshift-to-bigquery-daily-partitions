from sqlalchemy import create_engine


def connect(project, config):
    database='ef4'
    host=config['db_host'.format(proj=project)]
    port=config['db_port'.format(proj=project)]
    user=config['db_user'.format(proj=project)]
    password=config['db_password'.format(proj=project)]
    rs_cred = "postgresql://{user}:{passw}@{host}:{port}/{db}".format(
            host=host, port=port, user=user, passw=password, db=database)
    return create_engine(rs_cred)


def make_run(engine, schema):
    # database.schema.tablename
    def run_query(sql):
        with engine.connect() as conn:
            conn.execute("SET search_path TO {schema}".format(schema=schema))
            for result in conn.execute(sql):
                yield result
    return run_query
