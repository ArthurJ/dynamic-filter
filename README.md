# Dynamic Filter
This program receive a `data` file (at local filesystem or on hdfs) to be filter, it's `schema` in a separated file, and a `rule` file (at local filesystem only) with rules to be applied at the filtering process.

`spark-submit [spark options] dynamic_filter.py --data data_example.txt --rules rules.csv --schema schema_example.txt`

optional arguments:

    -h, --help                                                  show this help message and exit

    --ignore-types, -i                                          Ignore type enforcement step, all values will be saved as string

    --log-level {ALL,DEBUG,ERROR,FATAL,INFO,OFF,TRACE,WARN}     Log level options

Input Files:

    --data DATA, -d DATA                                        Text or gzip file to be filtered

    --rules RULES, -r RULES                                     File with the rules to be applied

    --schema SCHEMA, -s SCHEMA                                  File with the schema to be applied

Separator Types:

    --separator SEPARATOR, -S SEPARATOR                         Use the character as separator

    --width, -w                                                 Use fixed width to parse columns, ignoring separator

It uses Pyspark (Apache Spark 2.4+, Python 3.7+) to apply each rule to the data and saves 2 DataFrames: `valid_data`, with lines that complied with all rules and types enforced as described on schema, and `invalid_data` with all the other lines.

## Rules file format
- First line with a header (ignored), example: `RuleNumber,Condition`
- Other lines containing the rule name and a quoted string, separed by comma. Example: `rule1,"rlike '[A-F0-8]*'"`

Example:
```
RuleNumber,Condition
rule1,"rlike '[-+]?[0-9]+'"
rule2,"rlike '[0-9A-F]*'"
rule3,"in ('A','S','D','F')"
rule4,"rlike '(0[0-8]|1[1-9]|[2-6][0-9]|7[01])'"
```

## Data file format
- First line MUST have the correct fields' number if the values are separated by some character
- Data to be filtered.

Example:
```
UserCode,DateUpdate,UserName
12345,2018-01-01,Ana
13458,2018-02-01,Arthur
12645,2018-03-01,Alberto
```

or

```
123452018-01-01Ana
134582018-02-01Arthur
126452018-03-01Alberto
```

## Schema File
- First line with the header (ignored), example: `Columns,DataType,Rule,Width`
- Each line with: Column name, data type, rule name, width length (if needed).
- Each line MUST have 4 fields, separated by comma

Example:
```
Columns,DataType,Rule,Width
UserCode,IntegerType,rule1,5
DateUpdate,DateType,rule2,10
UserName,StringType,rule3,40
```

or

```
Columns,DataType,Rule,Width
UserCode,IntegerType,rule1,
DateUpdate,DateType,rule2,
UserName,StringType,rule3,
```