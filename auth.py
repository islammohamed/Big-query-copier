import httplib2
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
import settings
import os.path

def getGoogleAuthKey():
    AUTH_KEY = settings.OAUTH_AUTH_KEY;
    if not os.path.exists(AUTH_KEY):
        raise IOError('Google key file is not exists in this path %s' % AUTH_KEY)

    secretKeyFile = file(AUTH_KEY, 'rb')
    secretKey = secretKeyFile.read()
    secretKeyFile.close()

    return secretKey


'''
Do Authentication of Google API
'''
def doGoogleAuthentcation(secretKey):
    credentials = SignedJwtAssertionCredentials(settings.SERVICE_ACCOUNT_EMAIL, secretKey, scope='https://www.googleapis.com/auth/bigquery')
    http = httplib2.Http()
    http = credentials.authorize(http)
    return http
