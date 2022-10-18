import sqlalchemy as sa
import os

def get_engine(creds='default', verbose=False):
    ''' Returns a SQL Alchemy Engine

    https://docs.sqlalchemy.org/en/latest/core/engines.html

    TODO: Decide on conventions for specifying multiple sets of credentials
    in your environment. For now, only default will work, using BL's standard
    credential names.
    '''
    if creds == 'default':
        username = os.environ.get('DB_USERNAME')
        password = os.environ.get('DB_PASSWORD')
        host = os.environ.get('DB_HOST')
        port = os.environ.get('DB_PORT')
        database = os.environ.get('DB_DATABASE')
        driver = os.environ.get('DB_DRIVER')

    else:
        raise NotImplementedError

    return sa.create_engine(
        f"{driver}://{username}:{password}@{host}:{port}/{database}",
        echo=verbose
    )
