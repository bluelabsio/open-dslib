"""
Utilities for Querying and Loading Tables from redshift.
"""

from .engine import get_engine
import sqlalchemy as sa

import pandas as pd



def execute(query, connectable=None, verbose=False):
    """
    Convenience function that returns query results. Returns None if no results are returned.
    Returns an empty dataframe with column names if there are column names but no rows, and returns
    a dataframe if there are 1 or more rows.

    It allows for arbitrary execution of SQL statements in a single transaction block.

    TODO:
        [ ] add verbose options
        [ ] add some convention for communicating things like "TABLE DROPPED" or "NO TABLE DROPPED" etc.

    Args:
        query ():
        connectable ():
        verbose ():

    Returns:

    """
    if connectable is None:
        connectable = get_engine()

    results = connectable.execute(query)

    # The anon part is necessary because vertica's driver doesn't return a rowcount, and returns a column for drop/create statements
    if len(results.keys()) == 0 or (len(results.keys()) == 1 and results.keys()[0] == 'anon_1'):
        return None
    
    else:
        df = results.fetchall()
        
        if len(df) < 1:
            df = pd.DataFrame(columns=results.keys())
    
        else:
            df = pd.DataFrame(df, columns=results.keys())
    
    return df



class Connection(sa.engine.Connection):
    """
    This is a basic extension of: https://docs.sqlalchemy.org/en/latest/core/connections.html#sqlalchemy.engine.Connection
    It replaces the default execute functionality with the DS team's default execution behavior (i.e. returning a dataframe).
    """

    def __init__(self, creds='default', verbose=False):
        super().__init__(engine=get_engine(creds=creds, verbose=verbose))

    def execute(self, query):
        ''' Override default SA Execute Behavior for Consistency
        '''
        results = super().execute(query)

        # The anon part is necessary because vertica's driver doesn't return a rowcount, and returns a column for drop/create statements
        if len(results.keys()) == 0 or (len(results.keys()) == 1 and results.keys()[0] == 'anon_1'):
            return None

        else:
            df = results.fetchall()

            if len(df) < 1:
                df = pd.DataFrame(columns=results.keys())

            elif results.rowcount > 0:
                df = pd.DataFrame(df, columns=results.keys())

        return df



def connection(creds='default', verbose=False):
    """
    Returns a connection object a DS Team connection object

    Args:
        creds ():
        verbose ():

    Returns:

    """
    return Connection(creds=creds, verbose=verbose)

