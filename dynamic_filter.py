import csv
import argparse

#Parsing data file and rules dile
description=('This program receive a data file (at local '
             'filesystem or on hdfs) to be filter and a rule file '
             '(at local filesystem only) with rules to be applied '
             'at the filtering process.')
parser = argparse.ArgumentParser(prog='Dynamic Filter', 
                                 description=description)

group1 = parser.add_argument_group('Input Files')
group1.add_argument("--data", "-d", 
                    type=str,
                    required=True, 
                    help='Text or gzip file to be filtered')
group1.add_argument("--rules", "-r", 
                    type=str,
                    required=True, 
                    help='File with the rules to be applied')
group1.add_argument("--schema", "-s", 
                    type=str,
                    required=True, 
                    help='File with the schema to be applied')

group2 = parser.add_argument_group('Separator Types')
group2.add_argument('--separator', '-S', type=str, default = ',', 
                    help='Use the character as separator')
group2.add_argument('--width', '-w', action='store_true', 
                    help='Use fixed width to parse columns')

parser.add_argument("--ignore-types", "-i", 
                    action='store_true',
                    default=False,
                    help='Ignore type enforcement step, all values'
                         ' will be saved as string')
parser.add_argument("--log-level", "-l", 
                    choices=['ALL', 'DEBUG', 'ERROR', 'FATAL', 
                             'INFO', 'OFF', 'TRACE', 'WARN'], 
                    default='INFO',
                    help='Log level options')

args = parser.parse_args()

from pyspark.sql import functions as fun
from pyspark import Row, SparkConf, SparkContext
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame


conf = SparkConf().setAppName(parser.prog)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sparkContext=sc)
sc.setLogLevel(args.log_level)


### Reading files
data_rdd = sc.textFile(args.data)
rules = csv.reader(open(args.rules, 'r'), quotechar='"')
schema = open(args.schema, 'r').readlines()[1:]
schema = [line.split(',') for line in schema]
schema = [(name, data_type, rule_number, width.strip()) 
            for name, data_type, rule_number, width in schema]


### Preparing for parsing
columns = []
acc = 0
for name, _, _, width in schema:
    width = int(width or 0)
    columns.append((name, (acc, acc+width)))
    acc += width

if args.width:
    ### Parsing for fixed width based columns

    def parse(line, columns=columns):
        row = dict()
        for column, width in columns:
            # print('\t\t',column, width)
            row[column] = line[width[0]:width[1]]
        return Row(**row)
    
    data = data_rdd.map(parse).toDF()
else:
    ### Parsing Columns separated by character
    field_qty = len(data_rdd.first().split(args.separator))
    def parse(line, sep=args.separator):
        line = line.split(sep)
        return line if len(line) == field_qty else [None]*field_qty

    data = data_rdd.map(parse).toDF(schema=[c[0] for c in columns])


### Parsing Rules
rule_dict = dict()
for rule_number, validation in rules:
    #print(rule_number, validation)
    rule_dict[rule_number] = validation


### Enforcing Rules
valid_data = data.selectExpr('*')

conditions = []
for column, _, rule_number, _ in schema:
    condition = rule_dict.get(rule_number)
    print(f"{rule_number}",
          column.ljust(30),
          condition or 'Not Applied',
          sep='\t')
    if not condition:
        continue
    conditions.append(f"{column} {condition}")
conditions = ' and '.join(conditions)

if conditions:
    valid_data = valid_data.filter(conditions)


### Getting invalid values
invalid_data = data.exceptAll(valid_data)


### Enforcing data types
if not args.ignore_types:
    valid_data = valid_data.select(*[fun.col(x[0]).cast(eval(x[1])()) 
                                   for x in schema])


### Show off
valid_data.show(10)
invalid_data.show(10)


### Saving Data
executor_number = sc.defaultParallelism or 10
valid_data.repartition(executor_number).write.mode('overwrite')\
                                        .saveAsTable('valid_data')
invalid_data.repartition(executor_number).write.mode('overwrite')\
                                        .saveAsTable('invalid_data')
