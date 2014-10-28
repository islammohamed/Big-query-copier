from settings import AGGREGATE_QUERY_FILE, AGGREGATE_REQUEST_BODY, EXPORTED_QUERY_FILE
import auth
from client import execute_bigquery_query, execute_bigquery_insert
import os.path
import json
from StringIO import StringIO

def execute_aggregate_query(http):
    if not os.path.exists(AGGREGATE_REQUEST_BODY) or not os.path.exists(AGGREGATE_QUERY_FILE):
        raise Exception('aggregate query not exists in the path, please check the path in the settings.py file')

    '''
    load main request body and substitute the value of the query
    inside it.
    '''
    queryFile = open(AGGREGATE_REQUEST_BODY)
    queryBody = json.load(queryFile)

    sqlQueryFile = open(AGGREGATE_QUERY_FILE)
    sqlQuery = sqlQueryFile.read()
    sqlQueryFile.close()

    if queryBody['configuration']['query']['query']:
        queryBody['configuration']['query']['query'] = sqlQuery

    return execute_bigquery_insert(http, queryBody)


def import_intermediate_records(http):
    if not os.path.exists(EXPORTED_QUERY_FILE):
        raise Exception('intermedia query not exists in the path, please check the path in the settings.py file')

    intermediate_query_file = file(EXPORTED_QUERY_FILE)
    intermediate_query = intermediate_query_file.read()
    intermediate_query_file.close()

    return execute_bigquery_query(http, intermediate_query)
