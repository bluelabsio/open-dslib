# open-dslib
Open Source Data Science Modules

### Installation

Install using the following command:

```
pip install git+https://github.com/bluelabsio/open-dslib
```

### Usage

#### Environment Setup

The package picks up database credentials from the environment. A reference to the database name - eg. DATABASE - is required, and all environment variables must be named accordingly:

```
- {DATABASE}_DB - refers to the name of the database to be queried
- {DATABASE}_HOST - refers to the database host
- {DATABASE}_PORT - refers to the database port
- {DATABASE}_USER - refers to the database user making the connection
- {DATABASE}_PW - refers to the password for the database user who is making the connection
- {DATABASE}_TYPE - refers to the database dialect
```

#### Creating Crosstabs

Crosstabs can be created in two ways:

1. Passing all the crosstab parameters inline:

```
import dslib.assessment.crosstabs as xt

xtabs = xt.CrossTabs(
    database_name = 'DATABASE',
    universe="(SELECT * FROM schema.table_name LIMIT 100000)",
    metrics=[
        "count(*)",
        "COUNT(*)::FLOAT/NULLIF(SUM(COUNT(*)) OVER(), 0) prop",
    ],
    splits=[
        "gender",
        "race",
        ["gender", "race"] #(This can be used to cross two variables together)
    ]
)

xtabs.df
```

2. Passing crosstab parameters through a yaml config:

```
import dslib.assessment.crosstabs as xt

xtabs = xt.CrossTabs(
    database_name = 'DATABASE',
    yaml_file='example_config.yaml'
)

xtabs.df
```

The config should be formatted like:

```
universes:
    'Table1': (SELECT * FROM schema.table_name_1)
    'Table2': (SELECT * FROM schema.table_name_2)
    
metrics:
    - count(*)
    - COUNT(*)::FLOAT/NULLIF(SUM(COUNT(*)) OVER(), 0) prop
    
splits:
    - gender
    - race
    - - gender
      - race  #(This can be used to cross two variables together)
```

#### Naming a Universe

When providing multiple universes, provide a name for each universe that it can be referenced by. Ensure that you do not use any reserved words that might have some meaning int he SQL dialect you are working with. 

Examples of reserved words:
```
ALL
FULL
COLUMN
GROUP
```

You can find a list of reserved words for Amazon Redshift [here](https://docs.aws.amazon.com/redshift/latest/dg/r_pg_keywords.html).

Please do not provide any aliases for your universe in the sql that you enter, this will be handled by the tool itself.

