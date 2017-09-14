
import sys
import os
import datetime
from dateutil.relativedelta import relativedelta
from dateutil import parser

import pandas as pd
import numpy as np
import matplotlib.pyplot
from fbprophet import Prophet

import threading
from accounts import find_accounts, find_services

import boto3
from dynamodb import Dynamodb
import decimal


"""def find_max_end_time(cur):
    current_date = datetime.datetime.utcnow()
    prev_date = current_date + relativedelta(days=-1)
    sql = "select MAX(lineitem_usageenddate) enddate "
    sql += " from AWSBilling%s" % (prev_date.strftime('%Y%m'))
    sql += " where lineitem_usageenddate < '%s'" % (current_date.strftime('%Y-%m-%d'))
    cur.execute(sql)
    row = cur.fetchall()[0]
    print("the last available time is %s" % row[0])
    return parser.parse(row[0]).strftime('%H:%M')
"""

def draw(product_code):
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


def predict(df, product_code):

    product_df = df[df['lineitem_productcode'] == product_code]
    product_df = product_df.filter(items=['enddatetime', 'unblended'])

    prophet_df = pd.DataFrame({'ds': product_df['enddatetime'],
                    'y': product_df['unblended']})
    prophet_df = prophet_df[prophet_df['y'] > 0]
    if prophet_df.shape[0] == 0:
        print("No data in product %s" % product_code)
        return None

    current_date = datetime.datetime.utcnow()
    prev_date = current_date + relativedelta(days=-1)
    prev_date_str = prev_date.strftime("%Y-%m-%d")
    tail_date_str = parser.parse(prophet_df.tail(1)['ds'].iloc[0]).strftime("%Y-%m-%d")
    if prev_date_str != tail_date_str:
        print("The tail date, %s, is not same with the prev date, %s" % (tail_date_str, prev_date_str))
        return None

    """last_month = parser.parse(prophet_df.tail(1)['ds'].iloc[0]).strftime('%Y-%m')
    current_month = datetime.datetime.utcnow().strftime('%Y-%m')
    if (current_month != last_month):
        print('!!!current month: %s is not same with last data month: %s, so no need to predict' % (current_month, last_month))
        return None"""

    # change to stationary data
    prophet_df['y'] = np.log(prophet_df['y'])

    m = Prophet()

    try:
        m.fit(prophet_df);
        # if you have "data type "datetime" not understood" error, upgrade 'pandas' version to 0.20.3 to be exac
        # https://github.com/facebookincubator/prophet/issues/256

        # if there is any value of '0', you get "Rejecting initial value: Log probability evaluates to log(0)" error
        # https://github.com/facebookincubator/prophet/issues/232
    except Exception, ex:
        print('\n\n\n#############Exception during fit : %s' % ex)
        print(prophet_df)
        #raise ex
        print('\n\n\n')
        return None

    future = m.make_future_dataframe(periods=24, freq='H')
    #print future.tail(24)

    forecast = m.predict(future)
    predicted = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(24)
    print(predicted)
    yhat_exp_list = []
    yhat_lower_exp_list = []
    yhat_upper_exp_list = []
    for ir in predicted.itertuples():
        yhat_exp = np.exp(ir[2])
        yhat_lower_exp = np.exp(ir[3])
        yhat_upper_exp = np.exp(ir[4])
        print yhat_exp, yhat_lower_exp, yhat_upper_exp
        yhat_exp_list.append(yhat_exp)
        yhat_lower_exp_list.append(yhat_lower_exp)
        yhat_upper_exp_list.append(yhat_upper_exp)
    predicted['yhat_exp'] = yhat_exp_list
    predicted['yhat_lower_exp'] = yhat_lower_exp_list
    predicted['yhat_upper_exp'] = yhat_upper_exp_list
    print(predicted)
    return predicted


def save_predicted(account_id, product_code, predicted, dynamodb):
    for ir in predicted.itertuples():
        item = {
            'id': '%s_%s_%s' % (account_id, product_code, ir[1]),
            'account_id': account_id,
            'product_code': product_code,
            'datetime': ir[1].strftime('%Y-%m-%d %H:%M:%S'),
            'yhat': decimal.Decimal(format(ir[2], '.2f')),
            'yhat_lower': decimal.Decimal(format(ir[3], '.2f')),
            'yhat_upper': decimal.Decimal(format(ir[4], '.2f')),
            'yhat_exp': decimal.Decimal(format(ir[5], '.2f')),
            'yhat_lower_exp': decimal.Decimal(format(ir[6], '.2f')),
            'yhat_upper_exp': decimal.Decimal(format(ir[7], '.2f')),
            'time_stamp': int(ir[1].strftime("%s"))
        }
        dynamodb.create(item)


def predict_account(account_id, dynamodb):
    print("starting billing prediction of account %s" % account_id)
    #services = find_services(account_id)
    #print('services = %s' % services)

    # read all data files of this account
    current_date = datetime.datetime.utcnow()
    next_date = datetime.datetime(2016, 8, 1)
    filepath = './data/%s_%s.csv' % (account_id, next_date.strftime('%Y%m'))
    print("account, %s, reading a file, %s" % (account_id, filepath))
    try:
        df = pd.read_csv(filepath)
        print(df.shape)
    except Exception, ex:
        if os.path.isfile(filepath):
            raise ex
        else:
            print('file, %s, not exists, so skip it' % filepath)
            df = None
    while current_date.year != next_date.year or current_date.month != next_date.month:
        next_date = next_date + relativedelta(months=1)
        filepath = './data/%s_%s.csv' % (account_id, next_date.strftime('%Y%m'))
        print("account, %s, reading a file, %s" % (account_id, filepath))
        try:
            if df is not None:
                df = df.append(pd.read_csv(filepath))
            else:
                df = pd.read_csv(filepath)
            #print df.head()
            print(df.shape)
        except Exception, ex:
            if os.path.isfile(filepath):
                raise ex
            else:
                print('file, %s, not exists, so skip it' % filepath)

    if df is None:
        print("!!!There is no data file for account, %s" % account_id)
        return

    # data filtering
    if account_id == '054649790173':
        df = df[(df['lineitem_productcode'] != 'AmazonRedshift') | (df['enddatetime'] < '2016-11-07 23:00:00') | (df['enddatetime'] > '2016-12-19 18:00:00')]
        print(df.shape)

    product_codes = df.lineitem_productcode.unique()
    print('product_codes = %s' % product_codes)
    for product_code in product_codes:
        print(product_code)
        #if product_code not in services:
        #    print("!!!product '%s' is not a valid service in account '%s'" % (product_code, account_id))
        #    continue
        #draw(product_code)
        predicted = predict(df, product_code)
        if predicted is not None:
            save_predicted(account_id, product_code, predicted, dynamodb)
    print("completed billing prediction of account %s" % account_id)


"""max_end_time = find_max_end_time(cur)
if max_end_time != "23:00":
    print("The last time data is not available now, so don't predict yet")
    sys.exit()
"""

#threads = []
accounts = find_accounts()
print('\n\n***accounts = %s' % accounts)


region = os.environ.get('AWS_DEFAULT_REGION')
table_name = os.environ.get('DYNAMODB_TABLE_NAME')
client = boto3.resource('dynamodb', region_name=region)
dynamodb = Dynamodb(client, table_name)

#threads = []
for account_id in accounts:
    print(account_id)
    predict_account(account_id, dynamodb)
    #t = threading.Thread(target=predict_account, args=(account_id, ))
    #threads.append(t)
    #t.start()

#for thread in threads:
#    thread.join()
