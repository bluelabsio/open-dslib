from collections import OrderedDict
import yaml

import pandas as pd

def load_yaml(filename):
    
    with open(filename, 'r') as f:
        config = yaml.safe_load(f)

    return config


class Tab(object):
    ''' A tab object is the most atomic unit off the CrossTab. It must be executable
    in a single SQL statement, and only counts each member of the population once
    from a single universe. It can represent itself as SQL or an in-memory DataFrame.
    '''
    
    def __init__(self, universe_name, universe, metrics, splits=None,
                max_n_splits=1, rnk=-1, connectable=None,
                verbose=False, query_ob=None):
        ''' Initialize the Tab object.
        
        '''
        
        self._universe_name = universe_name
        self._universe = universe
        self._metrics = metrics
        self._splits = splits
        self._df = None
        
        self.rnk = rnk # Helps sort the crosstabs.
        self.max_n_splits = max_n_splits
        self.connectable = None
        self.verbose = False
        self.query = query_ob

    @property
    def universe_name(self):
        ''' Name of the universe object, used as an alias
        Universe name must be a string.
        '''
        if isinstance(self._universe_name, str):
            return self._universe_name.lower()
        else:
            raise TypeError("universe name must be a string")
    
        
    @property
    def universe(self):
        ''' Define the universe object.
        Universe can be either a string or a DataFrame.
        '''
        if isinstance(self._universe, pd.DataFrame):
            return self._universe
        elif isinstance(self._universe, str):
            return self._universe
        else:
            raise TypeError("universe must be either a dataframe or a string")
            
    @property
    def universe_sql(self):
        ''' Define the Universe SQL. If the object is a DataFrame,
        then call the universe "universe" otherwise return the given
        SQL.        
        '''
        if isinstance(self.universe, pd.DataFrame):
            return " universe "
        elif isinstance(self.universe, str):
            return self.universe
        else:
            raise TypeError("universe must be either a dataframe or a string")
    
    @property
    def splits(self):
        ''' Define what should be grouped over. Accepts nothing (topline),
        a string or a list of strings. Returns a string. If no string is
        provided t defaults to a Topline.
        '''
        if isinstance(self._splits, list):
            splits = self._splits
        elif isinstance(self._splits, str):
            splits = [self._splits]
        elif self._splits is None:
            splits = ["'Topline'"]
        else:
            raise TypeError("splits must be either a list of strings to split on or a single string")
            
        if len(splits) < self.max_n_splits:
            splits = splits + ["'-'"] * (self.max_n_splits - len(splits))
            
        return splits
            
    @property
    def splits_sql(self):
        ''' SQL representation of the split argument
        '''
        
        # Temporary hack to get CrossTabs working with BigQuery
        # Address the difference in SQL flavors
        from types import ModuleType
        if self.query is not None and type(self.query) == ModuleType and self.query.__name__.endswith('big_query'):
            string_type = 'STRING'
        else:
            string_type = 'VARCHAR'
        return ", ".join([f"CAST({split} AS {string_type}) as demo{i}" for i, split in enumerate(self.splits)])

    @property
    def splits_name(self):
        ''' Name of the split(s) to create alias for subquery
        '''
        if self._splits is None:
            return f"""{self.splits[0].replace("'", '')}""".lower()
        else:
            return f"""
            {'_x_'.join([split for split in self.splits if split != "'-'"])}""".lower()
        
    @property
    def splits_name_sql(self):
        ''' Name of the split(s), need to have return a statement
        with single quotes around it.
        '''
        if self._splits is None:
            return f"{self.splits[0]} as split"
        else:
            return f"""
            '{', '.join([split for split in self.splits if split != "'-'"])}' as split"""
        
    @property
    def metrics(self):
        ''' Parse the metrics. Metrics can be a single string (with one metric),
        or a list of strings. Returns a list of strings.
        '''
        if isinstance(self._metrics, list):
            for metric in self._metrics:
                if not isinstance(metric, str):
                    raise ValueError
            return self._metrics
        elif isinstance(self._metrics, str):
            return [self._metrics]
        else:
            raise TypeError("metrics must be a list of metrics or a string (single metric)")
            
    @property
    def metrics_sql(self):
        ''' The SQL representation of the chosen metrics. Basically just joining with commas.
        '''
        return ", ".join(self.metrics)
    
    @property
    def by_sql(self):
        n_groups = 1 # for rnk
        n_groups += 1 # for name
        n_groups += self.max_n_splits # Number of s
        return ",".join([f"{i}" for i in range(1, n_groups+1)])
        
    @property
    def sql(self):
        return f"""
        SELECT
            {self.rnk} as rnk,
            {self.splits_name_sql},
            {self.splits_sql},
            {self.metrics_sql}
        FROM {self.universe_sql} {self.universe_name}
        GROUP BY {self.by_sql}
        ORDER BY {self.by_sql}
        """
        
        
    
class CrossTabs(object):
    
    def __init__(self, universes=None, universe=None, metrics=None, splits=None,
                verbose=False, connectable=None, horizontal=True,
                yaml_file=None, query_ob=None):

        # Load configuration if it's specified.
        if yaml_file is not None:
            config = load_yaml(yaml_file)
            self.__init__(**config)
            self.yaml_file = yaml_file
            return None
            
        # Arguments:
        # Accept one argument for universe(s), but not both
        if universe is not None and universes is not None:
            raise ValueError("Please provide either a 'universe' argument or a 'universes' argument.")
        elif universe is not None:
            self._universes = universe
        else:
            self._universes = universes
        self._metrics = metrics
        self._splits = splits
        self.verbose = verbose
        self.connectable = connectable
        self.horizontal = horizontal

        # Initializing State
        self._tabs = None
        self._df = None
        
        if query_ob is None:
            import dslib.sql.query as default_query
            self.query = default_query
        else:
            self.query = query_ob

    def to_csv(self, name=None):
        
        if name is None:
            if self.yaml_file is not None:
                name = self.yaml_file.replace('.yaml', '.csv')
            else:
                raise TypeError("Please provide a name for your csv.")
        
        self.df.to_csv(name)
            
            
    @property
    def universes(self):
        
        if isinstance(self._universes, str):
            universes = OrderedDict({"universe": self._universes})
        
        elif isinstance(self._universes, dict):
            universes = OrderedDict(self._universes)
        
        else:
            raise TypeError("universe(s) must either be a string or a dictionary of strings")
            
        return universes
            
    @property
    def metrics(self):
        
        if isinstance(self._metrics, list):
            metrics = OrderedDict()
            for universe in self.universes:
                metrics[universe] = self._metrics

        elif isinstance(self._metrics, str):
            metrics = OrderedDict()
            for universe in self.universes:
                metrics[universe] = [self._metrics]
            
        elif isinstance(self._metrics, dict):
            metrics = self._metrics
        
        else:
            raise TypeError("metrics must a string(single metric), list of strings (multiple metrics for all universes), or dictionary of strings or a list of strings (multiple universes with different metrics)")
            
        return metrics
    
    @property
    def splits(self):
        
        splits = [None]
        
        if self._splits is None:
            pass
        
        elif isinstance(self._splits, str):
            splits.append(self._splits)
        
        elif isinstance(self._splits, list):
            splits += self._splits
            
        return splits

    @property
    def max_n_splits(self):
        return max([len(split) if isinstance(split, list) else 1 for split in self.splits])
    
    @property
    def tabs(self):
        
        if self._tabs is None:
            tabs = OrderedDict()
            for name, universe in self.universes.items():
                tabs[name] = OrderedDict()
                for i, split in enumerate(self.splits):
                    tab = Tab(universe_name=name, universe=universe, metrics=self.metrics[name],
                             splits=split, verbose=self.verbose,
                             connectable=self.connectable, max_n_splits=self.max_n_splits,
                             rnk=i, query_ob=self.query)
                    tabs[name][str(split)] = tab
            self._tabs = tabs

        return self._tabs
        
    @property
    def by_sql(self):
        return ",".join([f"{i}" for i in range(1, self.max_n_splits + 3)])
    
    @property
    def sql(self):
        sql = OrderedDict()
        
        for name, universe in self.universes.items():
            sql[name] = "SELECT a.* FROM ("
            
            sql[name] += " UNION ALL ".join([f"SELECT * FROM ({self.tabs[name][str(split)].sql}) {self.tabs[name][str(split)].splits_name}" for split in self.splits])
            
            sql[name] += f") a ORDER BY {self.by_sql};"
        
#         if len(self.universes) == 1:
#             return sql.popitem()[1]
        
        return sql
    
    @property
    def df(self):
        if self._df is None:
            # Populate Tab DFs
            dfs = [self.query.execute(sql, connectable=self.connectable) for universe,sql in self.sql.items()]

            # Return simple DF if only one:
            if len(dfs) == 1:
                self._df = dfs[0]
            
            # Join Data Horizontally, add column prefixes to designate universe:
            elif self.horizontal:
                df = dfs[0].iloc[:, :(2+self.max_n_splits)]
                # Columns to Join on
                join_cols = df.columns.values.tolist()
                # Loop through each universe
                for i, universe in enumerate(self.universes):
                    # Identify and rename metric columns
                    metric_cols = dfs[i].columns.values.tolist()[(2+self.max_n_splits):]
                    metric_cols = {f"{col}": f"{universe}_{col}" for col in metric_cols if universe != ''}
                    dfs[i].rename(index=str, columns=metric_cols, inplace=True)
                    # Merge Data to main df
                    df = df.merge(dfs[i], left_on=join_cols, right_on=join_cols, how='outer')
                self._df = df
                
            # Join Data Vertically:
            elif not self.horizontal:
                # Add universe column to differentiate universes
                for i, universe_name in enumerate(self.universes):
                    dfs[i].insert(1, 'universe', universe_name)
                # Concatenate DFs together
                df = pd.concat(
                    [df for df in dfs]
                , axis=0
                , ignore_index=True)
                self._df = df
                
        return self._df
