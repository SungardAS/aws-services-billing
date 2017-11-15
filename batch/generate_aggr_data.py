
import psycopg2

import sys
import os
import datetime
from dateutil.relativedelta import relativedelta
from dateutil import parser

#import threading
from accounts import find_accounts #, find_services

import boto3
from dynamodb import Dynamodb
import decimal


def save_aggr_data(row, dynamodb):
    # lineitem_usageaccountid | lineitem_productcode |  last_billing_date   |     blended      |    unblended
    item = {
        'id': '%s_%s' % (row[0], parser.parse(row[2]).strftime('%Y-%m')),
        'account_id': row[0],
        'product_code': row[1],
        'datetime': parser.parse(row[2]).strftime('%Y-%m-%d %H:%M:%S'),
        'blended': decimal.Decimal(format(row[3], '.2f')),
        'unblended': decimal.Decimal(format(row[4], '.2f')),
        'time_stamp': int(parser.parse(row[2]).strftime("%s"))
    }
    dynamodb.create(item)


def generate_aggr_data(cur, dynamodb, current_date, account_id=None):

    print("starting to generate billing aggr data of account %s" % account_id)

    # first get aggr data for all services
    if account_id:
        sql = "select lineItem_UsageAccountId, '*' lineitem_productcode,"
        sql += " max(lineitem_usageenddate) last_billing_date,"
        sql += " sum(cast(lineItem_BlendedCost as float)) blended,"
        sql += " sum(cast(lineitem_unblendedcost as float)) unblended"
        sql += " from AWSBilling%s" % (current_date.strftime('%Y%m'))
        sql += " where lineitem_usageenddate < '%s'" % ((current_date + relativedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))
        sql += " and lineItem_UsageAccountId = '%s'" % (account_id)
        sql += " group by lineItem_UsageAccountId"
    else:
        sql = "select '*' lineItem_UsageAccountId, '*' lineitem_productcode,"
        sql += " max(lineitem_usageenddate) last_billing_date,"
        sql += " sum(cast(lineItem_BlendedCost as float)) blended,"
        sql += " sum(cast(lineitem_unblendedcost as float)) unblended"
        sql += " from AWSBilling%s" % (current_date.strftime('%Y%m'))
        sql += " where lineitem_usageenddate < '%s'" % ((current_date + relativedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))
    print(sql);
    cur.execute(sql)
    for row in cur.fetchall():
        save_aggr_data(row, dynamodb)

    # now get aggr data for each service
    if account_id:
        sql = "select lineItem_UsageAccountId, lineitem_productcode,"
        sql += " max(lineitem_usageenddate) last_billing_date,"
        sql += " sum(cast(lineItem_BlendedCost as float)) blended,"
        sql += " sum(cast(lineitem_unblendedcost as float)) unblended"
        sql += " from AWSBilling%s" % (current_date.strftime('%Y%m'))
        sql += " where lineitem_usageenddate < '%s'" % ((current_date + relativedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))
        sql += " and lineItem_UsageAccountId = '%s'" % (account_id)
        sql += " group by lineItem_UsageAccountId, lineitem_productcode"
        sql += " order by lineItem_UsageAccountId, lineitem_productcode"
    else:
        sql = "select '*' lineItem_UsageAccountId, lineitem_productcode,"
        sql += " max(lineitem_usageenddate) last_billing_date,"
        sql += " sum(cast(lineItem_BlendedCost as float)) blended,"
        sql += " sum(cast(lineitem_unblendedcost as float)) unblended"
        sql += " from AWSBilling%s" % (current_date.strftime('%Y%m'))
        sql += " where lineitem_usageenddate < '%s'" % ((current_date + relativedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))
        sql += "group by lineitem_productcode"
        sql += " order by lineitem_productcode"
    print(sql);
    cur.execute(sql)
    for row in cur.fetchall():
        save_aggr_data(row, dynamodb)

    print("completed to generate billing aggr data of account %s" % account_id)


if __name__ == "__main__":

    host = os.environ.get('REDSHIFT_HOST_NAME')
    dbname = os.environ.get('REDSHIFT_DATABASE_NAME')
    port = os.environ.get('REDSHIFT_DATABASE_PORT')
    user = os.environ.get('REDSHIFT_USER_NAME')
    pwd = os.environ.get('REDSHIFT_PASSWORD')

    con = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=pwd)
    cur = con.cursor()

    region = os.environ.get('AWS_DEFAULT_REGION')
    table_name = os.environ.get('DYNAMODB_AGGR_TABLE_NAME')
    client = boto3.resource('dynamodb', region_name=region)
    dynamodb = Dynamodb(client, table_name)

    #threads = []
    accounts = find_accounts()
    print('\n\n***accounts = %s' % accounts)

    if len(sys.argv) == 1:
        current_date = datetime.datetime.utcnow()
    else:
        current_date = parser.parse(sys.argv[1])
        current_date = datetime.datetime(current_date.year, current_date.month, 1)
        current_date = current_date + relativedelta(months=1) + relativedelta(days=-1)
    print("cuurent_date = %s" % current_date)

    # first, store sum of all accounts
    generate_aggr_data(cur, dynamodb, current_date)

    #threads = []
    for account_id in accounts:
        print(account_id)
        generate_aggr_data(cur, dynamodb, current_date, account_id)
        #t = threading.Thread(target=predict_account, args=(account_id, ))
        #threads.append(t)
        #t.start()

    #for thread in threads:
    #    thread.join()

    cur.close()
    con.close()
