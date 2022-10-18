# open-dslib
Open Source Data Science Modules

### Installation

Install using the following command:

```
pip install git+https://github.com/bluelabsio/open-dslib
```

### Usage

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

