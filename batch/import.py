
import psycopg2

import os
import datetime
from dateutil.relativedelta import relativedelta
#from dateutil import parser

#import threading
from accounts import find_accounts #, find_services

import boto3
from dynamodb import Dynamodb
import decimal


def build_sql(from_date, to_date, table_date):
    sql = "select cast(lineitem_usageenddate as datetime) enddatetime, lineItem_UsageAccountId, lineitem_productcode,"
    #sql += " product_servicecode, lineitem_operation, lineitem_usagetype,"
    sql += " sum(lineItem_UsageAmount) usage_amount,"
    sql += " to_char(sum(cast(lineItem_BlendedCost as float)), 'FM999,999,999,990D00') blended,"
    sql += " to_char(sum(cast(lineitem_unblendedcost as float)), 'FM999,999,999,990D00') unblended"
    sql += " from AWSBilling%s" % (table_date.strftime('%Y%m'))
    sql += " where lineitem_lineitemtype = 'Usage'"
    sql += " and lineitem_usageaccountid = '%s'" %(account_id)
    sql += " and lineitem_usageenddate >= '%s'" % (from_date.strftime('%Y-%m-%d'))
    sql += " and lineitem_usageenddate < '%s'" % (to_date.strftime('%Y-%m-%d'))
    sql += " group by enddatetime, lineItem_UsageAccountId, lineitem_productcode"
    #sql += ", product_servicecode, lineitem_operation, lineitem_usagetype"
    sql += " order by enddatetime, lineItem_UsageAccountId, lineitem_productcode"
    return sql


def import_billing(cur, account_id, dynamodb):

    print("starting filling today's billing data of account %s" % account_id)

    current_date = datetime.datetime.utcnow()
    prev_date = current_date + relativedelta(days=-1)
    next_date = current_date + relativedelta(days=1)

    """
    sql = "select cast(lineitem_usageenddate as datetime) enddatetime, lineItem_UsageAccountId, lineitem_productcode,"
    #sql += " product_servicecode, lineitem_operation, lineitem_usagetype,"
    sql += " sum(lineItem_UsageAmount) usage_amount,"
    sql += " to_char(sum(cast(lineItem_BlendedCost as float)), 'FM999,999,999,990D00') blended,"
    sql += " to_char(sum(cast(lineitem_unblendedcost as float)), 'FM999,999,999,990D00') unblended"
    sql += " from AWSBilling%s" % (prev_date.strftime('%Y%m'))
    sql += " where lineitem_lineitemtype = 'Usage'"
    sql += " and lineitem_usageaccountid = '%s'" %(account_id)
    sql += " and lineitem_usageenddate >= '%s'" % (prev_date.strftime('%Y-%m-%d'))
    sql += " and lineitem_usageenddate < '%s'" % (next_date.strftime('%Y-%m-%d'))
    sql += " group by enddatetime, lineItem_UsageAccountId, lineitem_productcode"
    #sql += ", product_servicecode, lineitem_operation, lineitem_usagetype"
    sql += " order by enddatetime, lineItem_UsageAccountId, lineitem_productcode"
    #sql += ", product_servicecode, lineitem_operation, lineitem_usagetype"
    """
    sql = build_sql(prev_date, next_date, prev_date)
    #print(sql);
    cur.execute(sql)
    rows = cur.fetchall()
    save_billing(rows, dynamodb)

    if prev_date.month != next_date.month and next_date.month == current_date.month:
        sql = build_sql(prev_date, next_date, next_date)
        #print(sql);
        cur.execute(sql)
        rows = cur.fetchall()
        save_billing(rows, dynamodb)


    print("completed filling today's billing data of account %s" % account_id)


def save_billing(rows, dynamodb):
    for row in rows:
        print("%s,%s,%s,%s,%s,%s" % (row[0], row[1], row[2], row[3], row[4], row[5]))
        id = '%s_%s_%s' % (row[1], row[2], row[0])
        blended = decimal.Decimal(row[4])
        unblended = decimal.Decimal(row[5])
        print('id: %s' % id)
        item = dynamodb.find_by_id(id)
        if item:
            print('item found %s' % item)
            item['usage_amount'] = row[3]
            item['blended'] = blended
            item['unblended'] = unblended
            dynamodb.update(item)
        else:
            if blended == 0 or unblended == 0:
                print('!!!blended or unblended is 0, so no need to create')
            else:
                item = {
                    'id': '%s_%s_%s' % (row[1], row[2], row[0]),
                    'account_id': row[1],
                    'product_code': row[2],
                    'datetime': '%s' % row[0],
                    'usage_amount': row[3],
                    'blended': blended,
                    'unblended': unblended,
                    'time_stamp': row[0].strftime("%s")
                }
                print('add a new item %s' % item)
                dynamodb.create(item)


def import_account(cur, account_id, dynamodb):
    print("starting billing import of account %s" % account_id)
    predicted = import_billing(cur, account_id, dynamodb)
    if predicted is not None:
        save_predicted(account_id, product_code, predicted, dynamodb)
    print("completed billing import of account %s" % account_id)


host = os.environ.get('REDSHIFT_HOST_NAME')
dbname = os.environ.get('REDSHIFT_DATABSE_NAME')
port = os.environ.get('REDSHIFT_DATABSE_PORT')
user = os.environ.get('REDSHIFT_USER_NAME')
pwd = os.environ.get('REDSHIFT_PASSWORD')

con = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=pwd)
cur = con.cursor()

region = os.environ.get('AWS_DEFAULT_REGION')
table_name = os.environ.get('DYNAMODB_TABLE_NAME')
client = boto3.resource('dynamodb', region_name=region)
dynamodb = Dynamodb(client, table_name)

accounts = find_accounts()
print('\n\n***accounts = %s' % accounts)

#threads = []
for account_id in accounts:
    print(account_id)
    import_account(cur, account_id, dynamodb)
    #t = threading.Thread(target=predict_account, args=(account_id, ))
    #threads.append(t)
    #t.start()

#for thread in threads:
#    thread.join()

"""
account_id = '427004835786'
import_account(cur, account_id, dynamodb)
"""

cur.close()
con.close()
