
import psycopg2

#import sys
import os
import datetime
from dateutil.relativedelta import relativedelta
#from dateutil import parser

#import pandas as pd
#import numpy as np
#import matplotlib.pyplot
#from fbprophet import Prophet

#import threading
from accounts import find_accounts #, find_services

#import boto3
#from dynamodb import Dynamodb
#import decimal

import_all = os.environ.get('BILLING_IMPORT_ALL_DATA')


def find_billing(cur, account_id):

    print("starting billing import of account %s" % account_id)

    #fwrite = open('./data/%s.csv' % (account_id), 'w')

    current_date = datetime.datetime.utcnow()
    #current_date = datetime.datetime(2017,3,1)
    if import_all == 'True':
        start_date = datetime.datetime(2016, 8, 1)
        #start_date = current_date + relativedelta(months=-2)
    else:
        start_date = datetime.datetime(current_date.year, current_date.month, 1)

    while current_date.date() > start_date.date():
        if current_date.year == start_date.year and current_date.month == start_date.month:
            next_date = datetime.datetime(current_date.year, current_date.month, current_date.day)
            if next_date < start_date:
                break
        else:
            #next_date = start_date + datetime.timedelta(days=31)
            next_date = start_date + relativedelta(months=1)
            next_date = datetime.datetime(next_date.year, next_date.month, 1)

        filepath = './data/%s_%s.csv' % (account_id, start_date.strftime('%Y%m'))
        print('filepath = %s' % filepath)
        fwrite = open(filepath, 'w')
        fwrite.write("enddatetime,lineItem_UsageAccountId,lineitem_productcode,usage_amount,blended,unblended\n")
        sql = "select cast(lineitem_usageenddate as datetime) enddatetime, lineItem_UsageAccountId, lineitem_productcode,"
        #sql += " product_servicecode, lineitem_operation, lineitem_usagetype,"
        sql += " sum(lineItem_UsageAmount) usage_amount,"
        sql += " to_char(sum(cast(lineItem_BlendedCost as float)), 'FM999,999,999,990D00') blended,"
        sql += " to_char(sum(cast(lineitem_unblendedcost as float)), 'FM999,999,999,990D00') unblended"
        sql += " from AWSBilling%s" % (start_date.strftime('%Y%m'))
        sql += " where lineitem_lineitemtype = 'Usage'"
        sql += " and lineitem_usageaccountid = '%s'" %(account_id)
        sql += " and lineitem_usageenddate < '%s'" % (next_date.strftime('%Y-%m-%d'))
        sql += " group by enddatetime, lineItem_UsageAccountId, lineitem_productcode"
        #sql += ", product_servicecode, lineitem_operation, lineitem_usagetype"
        sql += " order by enddatetime, lineItem_UsageAccountId, lineitem_productcode"
        #sql += ", product_servicecode, lineitem_operation, lineitem_usagetype"
        #print(sql);
        """print('start_date : %s' % start_date)
        print('next_date : %s' % next_date)
        start_date = next_date"""

        cur.execute(sql)
        #data = [("%s,%s" % (str(a[0]), a[1])) for a in cur.fetchall()]
        #csv_data = "\n".join(data)
        #cur.fetchall()
        for row in cur.fetchall():
            row4 = row[4]
            row5 = row[5]
            if isinstance(row4, str):
                #print(row4)
                row4 = float(row4.replace(',', ''))
            if isinstance(row5, str):
                row5 = float(row5.replace(',', ''))
            fwrite.write("%s,%s,%s,%s,%s,%s\n" % (str(row[0]), row[1], row[2], str(row[3]), row4, row5))
        fwrite.close()

        start_date = next_date

    #fwrite.close()

    print("completed billing import of account %s" % account_id)


"""def draw(product_code):
    product_code_df = df[df['lineitem_productcode'] == product_code]
    product_code_df = product_code_df.filter(items=['enddatetime', 'usage_amount'])
    #print(product_code_df.columns.values)
    #print(product_code_df.describe())
    #print(product_code_df.keys())

    dates = [pd.to_datetime(d) for d in product_code_df.iloc[:,0]]
    x = dates
    y = product_code_df['usage_amount']
    matplotlib.pyplot.figure(figsize=(12,6))
    matplotlib.pyplot.scatter(x,y)
    matplotlib.pyplot.show()
    #product_code_df['enddatetime'] = dates
    #print(product_code_df.head())
    #product_code_df.plot(kind='scatter', x='enddatetime', y='usage_amount', figsize=(12,8))

    #print(product_code_df.groupby(['enddatetime'], as_index=False).mean().sort_values(by='usage_amount', ascending=False))
"""

host = os.environ.get('REDSHIFT_HOST_NAME')
dbname = os.environ.get('REDSHIFT_DATABSE_NAME')
port = os.environ.get('REDSHIFT_DATABSE_PORT')
user = os.environ.get('REDSHIFT_USER_NAME')
pwd = os.environ.get('REDSHIFT_PASSWORD')

con = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=pwd)
cur = con.cursor()

#threads = []
accounts = find_accounts()
print('\n\n***accounts = %s' % accounts)


for account_id in accounts:
    find_billing(cur, account_id)
    #t = threading.Thread(target=find_billing, args=(cur, account_id, ))
    #threads.append(t)
    #t.start()

#for thread in threads:
#    thread.join()

cur.close()
con.close()
