
from atlassian import Jira
import pandas as pd
import json
import os
import avro.schema
import csv
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import sys
import re

pd.set_option('max_colwidth', 140)
pd.set_option("display.max_rows", None, "display.max_columns", 4)
csv.field_size_limit(sys.maxsize)

USERNAME = os.getenv('USERNAME')
PWD = os.getenv('PWD')
URL = os.getenv('URL')

jira_instance = Jira(
    url=URL",
    username=USERNAME,
    password=PWD,
    verify_ssl=False
)

PROJECTS = ['Project A', 'Project B', 'Project C', 'Project D']
FIELDS = '*all'

def retrieve_all_query_results(jira_instance: Jira, query_string: str, fields: list) -> list:
    issues_per_query = 1000
    list_of_jira_issues = []

    num_issues_in_query_result_set = jira_instance.jql(query_string, limit=0)["total"]
    print(f"Query `{query_string}` returns {num_issues_in_query_result_set} issues")

    for query_number in range(0, (num_issues_in_query_result_set // issues_per_query) + 1):
        results = jira_instance.jql(query_string, limit=issues_per_query, start=query_number * issues_per_query,
                                    fields=fields)
        list_of_jira_issues.extend(results["issues"])

    return list_of_jira_issues


jql_result_set_issues = retrieve_all_query_results(jira_instance, "Project in ('Project A', 'Project B', 'Project C', 'Project D')",
                                                   fields=FIELDS)

custom_fields_list = jira_instance.get_all_custom_fields()
custom_fields_df = pd.json_normalize(custom_fields_list)
custom_fields_df["name"] = custom_fields_df["name"].str.replace(' ', '_') \
                                                    .str.replace('(', '_') \
                                                    .str.replace(')', '_') \
                                                    .str.replace('/', '_') \
                                                    .str.replace('#', '_') \
                                                    .str.replace('?', '_') \
                                                    .str.replace('-', '_') \
                                                    .str.replace('$', '_')
custom_fields_df["name"] = custom_fields_df["name"].str.lower()
custom_fields_df = custom_fields_df.filter(['id', 'name'])
custom_fields_df.reset_index(drop=True, inplace=True)

custom_fields_map = custom_fields_df.set_index('id').T.to_dict()

df = pd.json_normalize(jql_result_set_issues)
df.columns = [x.replace(".", "_") for x in df.columns]
df.columns = [x.lower() for x in df.columns]
df.reset_index(drop=True, inplace=True)
df = df.rename(columns=lambda x: x.replace("fields_", ""))

for column_name in df:
    custom_fields_map_list = list(custom_fields_map.keys())

    def get_itm(column_name):
        for itm in custom_fields_map_list:
            if itm in column_name:
                return itm

    if any(itm in column_name for itm in list(custom_fields_map.keys())):
        print(column_name + " -> " + re.sub(get_itm(column_name), custom_fields_map[get_itm(column_name)]['name'], column_name))
        df = df.rename(columns={column_name: re.sub(get_itm(column_name), custom_fields_map[get_itm(column_name)]['name'], column_name)})

## fix to remove dupe
d1 = {'vendor_name': ['vendor_name_1', 'vendor_name_2']}
df = df.rename(columns=lambda c: d1[c].pop(0) if c in d1.keys() else c)
d2 = {'target_date': ['target_date_1', 'target_date_2']}
df = df.rename(columns=lambda c: d2[c].pop(0) if c in d2.keys() else c)

df.to_csv('jira.csv', index=False)


def get_name_type_pairs(header):
    return ",\n".join(
        ['\t\t{"name": "%s", "type": "string"}' % x.replace(' ', '_').replace('(', '_').replace(')', '_') for x in
         header])


def generate_schema(header):
    schema_str = """{
                            "namespace": "%s",
                            "type": "record",
                            "name": "Log",
                            "fields": [
                        %s
                            ]
                        }""" % ("result", get_name_type_pairs(header))
    return avro.schema.parse(schema_str)

with open('jira.csv', 'r', encoding='utf-8-sig', newline='') as opened_in_file:
    reader = csv.DictReader(opened_in_file, dialect="excel")

    header = [x.replace(' ', '_').replace('(', '_').replace(')', '_').replace('/', '_') for x in reader.fieldnames]
    reader = csv.DictReader(opened_in_file, dialect="excel", fieldnames=header)
    print(str(header))
    avro_schema = generate_schema(header)

    with open("jira.avro", 'wb') as opened_out_file:
        writer = DataFileWriter(opened_out_file, DatumWriter(), avro_schema)
        for line in reader:
            try:
                writer.append(line)
            except Exception as e:
                print("Error: %s for line %s" % (e, line))
        writer.close()

    avro_reader = avro.datafile.DataFileReader(open('jira.avro', "rb"),
                                               avro.io.DatumReader())

    result_avsc: dict = json.loads(avro_reader.meta.get('avro.schema').decode('utf-8'))
    result_avsc['name'] = 'jira'

   # print("result_avsc::")
   # print(result_avsc)
