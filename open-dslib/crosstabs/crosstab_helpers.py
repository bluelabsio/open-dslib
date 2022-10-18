def mean(column, weight=None, name=None):
    
    if name is None:
        name = f'avg_{column}'
        
        if weight is not None:
            name = 'w' + name
    
    if weight is None:
        return f'AVG(CAST({column} as REAL)) {name}'
    
    if weight is not None:
        return f'CAST(SUM({column} * {weight}) as REAL)/SUM(CASE WHEN {column} IS NOT NULL THEN {weight} ELSE NULL END) {name}'

def avg(column, weight=None, name=None):
    return mean(column=column, weight=weight, name=name)
    
def count(column=None, distinct=False, name=None):
    
    if name is None:
        name = f'cnt_{column}'
            
    if column is None:
        return f'count(*) {name}'
    
    if column is not None:
        return f'count({column}) {name}'
    
    if column is not None and distinct:
        return f'count(distinct({column})) {name}'
    
    
def sum(column, name=None):
    if name is None:
        name = f'sum_{column}'
        
    return f'SUM({column}) {name}'
        
def pct(column, weight=None, over=None, name=None):
    
    if name is None:    
        name = f'pct_{column}'
        
        if weight is not None:
            name = 'w' + name
            
    if over is None:
        over = ''
    elif isinstance(over, list):
        over = ",".join(over)
    
    if weight is None:
        over = ''
        return f'CAST(SUM({column}) as REAL) / SUM(SUM({column})) OVER({over}) {name}'
    
    if weight is not None:
        over = ''
        return f'CAST(SUM({column} * {weight}) AS REAL) / SUM(SUM({column} * {weight})) OVER({over}) {name}'
    