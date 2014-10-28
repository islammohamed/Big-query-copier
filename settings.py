import os

#settings of import
MAIN_PATH = os.path.realpath(__file__)
AGGREGATE_QUERY_FILE =  'query/aggregate.sql'
AGGREGATE_REQUEST_BODY = 'query/aggregate.query.json'
EXPORTED_QUERY_FILE =  'query/exported.sql'

#auth data
OUATH_DATA_JAM = 'jam/gquery_credentials.dat'
OAUTH_AUTH_KEY = 'keys/google.key'

#local mysql credentials
MYSQL_SERVER = 'localhost'
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = '123456'
MYSQL_PORT = 3306

#database import table
MYSQL_LOCAL_EXPORT_TABLE = 'bigdata_imported';
MYSQL_LOCAL_EXPORT_DB = 'bigdata';

PROJECT_NUMBER = 'panda-premium-analytics'
SERVICE_ACCOUNT_EMAIL = '74417662869-t6cq48kt8k98lebvq2nu9j4pltpvj8p9@developer.gserviceaccount.com'
