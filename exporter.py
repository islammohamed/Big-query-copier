import MySQLdb
import settings

def connect_to_mysql():
    connection = MySQLdb.connect(user= settings.MYSQL_USERNAME, passwd=settings.MYSQL_PASSWORD, db=settings.MYSQL_LOCAL_EXPORT_DB, host = settings.MYSQL_SERVER)
    connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    return connection

def get_columns(data):
    columns = []
    for column in data:
        columns.append(column['name'])

    return columns

def export_to_mysql(data):
    if len(data) == 0:
        raise Exception('no data to export')

    connection = connect_to_mysql()
    if connection:
        sqlQuery = "insert into {table_name} (country, visitor_id, visit_number, visit_id, date, visit_start_time, total_visits, total_pageviews, total_transactions, total_transaction_revenue, total_new_visits, referral_path, campaign, source, medium, keyword, adContent, transaction_id) VALUES ('{country}', '{visitor_id}' ,'{visit_number}','{visit_id}', '{date}', '{visit_start_time}', '{total_visits}', '{total_pageviews}', '{total_transactions}', '{total_transaction_revenue}', '{total_new_visits}', '{referral_path}', '{campaign}','{source}', '{medium}', '{keyword}', '{adContent}', '{transaction_id}');"

        columns = get_columns(data['schema']['fields'])
        for row in data['rows']:
            rowValues = row['f']
            data_dict = {}
            for index, prop in enumerate(rowValues):
                propertyValue = prop['v']
                if propertyValue:
                    propertyValue = MySQLdb.escape_string(propertyValue.encode('utf-8'))

                data_dict[columns[index]] = propertyValue

            dbquery = sqlQuery.format(table_name= settings.MYSQL_LOCAL_EXPORT_TABLE, **data_dict)
            connection.cursor().execute(dbquery)

        connection.commit()


