"""
Get an appropriate BlueLabs Joblib or SQL Alchemy Engine
"""
import getpass
import os
import warnings

# with warnings.catch_warnings():
#     warnings.filterwarnings("ignore")
#     from records_mover import Session

import sqlalchemy as sa

options = dict()
options['default_database'] = 'default'

def set_engine(creds='default'):
    options['default_database'] = creds

def get_engine(creds=None, verbose=False):
    """

    Args:
        creds ():
        verbose ():

    https://docs.sqlalchemy.org/en/latest/core/engines.html

    Returns:

    """

    if creds == None:
        creds = options['default_database']

    # session = Session()
    # if creds == 'default':
    #     return session.get_default_db_engine()
    if getpass.getuser() == 'jovyan':
        if creds == 'dnc':
            driver = 'vertica+vertica_python'
            username = os.environ.get('DNC_DB_USERNAME')
            password = os.environ.get('DNC_DB_PASSWORD')
            host = os.environ.get('DNC_DB_HOST')
            port = os.environ.get('DNC_DB_PORT')
            database = os.environ.get('DNC_DB_DATABASE')
            return sa.create_engine(
                f"{driver}://{username}:{password}@{host}:{port}/{database}",
                echo=verbose
            )
        elif creds == 'redshift':
            driver = 'redshift'
            username = os.environ.get('REDSHIFT_DB_USERNAME')
            password = os.environ.get('REDSHIFT_DB_PASSWORD')
            host = os.environ.get('REDSHIFT_DB_HOST')
            port = os.environ.get('REDSHIFT_DB_PORT')
            database = os.environ.get('REDSHIFT_DB_DATABASE')
            return sa.create_engine(
                f"{driver}://{username}:{password}@{host}:{port}/{database}",
                echo=verbose
            )
        elif creds == 'influencers':
            driver = 'redshift'
            username = os.environ.get('INFLUENCERS_DB_USERNAME')
            password = os.environ.get('INFLUENCERS_DB_PASSWORD')
            host = os.environ.get('INFLUENCERS_DB_HOST')
            port = os.environ.get('INFLUENCERS_DB_PORT')
            database = os.environ.get('INFLUENCERS_DB_DATABASE')
            return sa.create_engine(
                f"{driver}://{username}:{password}@{host}:{port}/{database}",
                echo=verbose
            )
        elif creds == 'postgres':
            driver = 'postgresql'
            username = os.environ.get('POSTGRES_DB_USERNAME')
            password = os.environ.get('POSTGRES_DB_PASSWORD')
            host = os.environ.get('POSTGRES_DB_HOST')
            port = os.environ.get('POSTGRES_DB_PORT')
            database = os.environ.get('POSTGRES_DB_DATABASE')
            return sa.create_engine(
                f"{driver}://{username}:{password}@{host}:{port}/{database}",
                echo=verbose
            )
        elif creds == 'default':
            driver = 'redshift'
            username = os.environ.get('DB_USERNAME')
            password = os.environ.get('DB_PASSWORD')
            host = os.environ.get('DB_HOST')
            port = os.environ.get('DB_PORT')
            database = os.environ.get('DB_DATABASE')
            return sa.create_engine(
                f"{driver}://{username}:{password}@{host}:{port}/{database}",
                echo=verbose
            )

    else:
        return session.get_db_engine(creds)



"""
In case you need to reference this to make your own engine at some point:
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

    else:
        raise NotImplementedError

    return sa.create_engine(
        f"redshift://{username}:{password}@{host}:{port}/{database}",
        echo=verbose
    )
"""
