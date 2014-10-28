import foodpanda_query
import auth
import logging
from exporter import export_to_mysql

def main():
    #authenticate google
    google_key = auth.getGoogleAuthKey()
    http = auth.doGoogleAuthentcation(google_key)

    #first aggregate the big query to intermediate table
    foodpanda_query.execute_aggregate_query(http)

    #get the data from the intermediate/ready table
    export_to_mysql(foodpanda_query.import_intermediate_records(http))

if __name__ == '__main__':
    main()
