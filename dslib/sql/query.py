"""
Utilities for Querying and Loading Tables from redshift.
"""
from .engine import EngineContext
import sqlalchemy as sa

import pandas as pd



def execute(query, database_name='DB', driver=None, verbose=False):
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
        database_name ():
        driver ():
        verbose ():

    Returns:

    """

    with EngineContext(name=database_name, driver=driver) as engine:
        connection = engine.create_connection()
        result = connection.execute(query)

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
