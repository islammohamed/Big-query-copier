import httplib2
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
import settings
import os.path
import logging

def waitforJobtoBeDone(http,timeout,  projectId, jobId):
    queryReply =  get_service_jobs().getQueryResults(projectId=projectId, jobId=jobId, timeoutMs=timeout).execute(http)
    while not queryReply['jobComplete']:
        logging.info('Job not yet complete...')
        queryReply = get_service_jobs().getQueryResults(projectId=projectId, jobId=jobId, timeoutMs=timeout).execute(http)

    return queryReply

def execute_bigquery_query(http, queryString):
    query_data = {'query':queryString}
    queryReply = get_service_jobs().query(projectId=settings.PROJECT_NUMBER, body=query_data).execute(http)

    jobReference=queryReply['jobReference']
    timeout = 10
    if not queryReply['jobComplete']:
         queryReply = waitforJobtoBeDone(http, timeout, jobReference['projectId'], jobReference['jobId'])
    return queryReply

def get_service_jobs():
    service = build('bigquery', 'v2')
    query_request = service.jobs()
    logging.info('Sending query to bigquery')
    return query_request


def execute_bigquery_insert(http, query):
    queryReply = get_service_jobs().insert(projectId=settings.PROJECT_NUMBER, body=query).execute(http)
    timeout = 10
    jobReference=queryReply['jobReference']
    '''
        this process will keep blocking until getting the query reply
    '''
    if queryReply['status']['state'] == 'RUNNING':
        queryReply = waitforJobtoBeDone(http, timeout, jobReference['projectId'], jobReference['jobId'])
    return queryReply
