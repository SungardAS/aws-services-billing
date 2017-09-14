
from __future__ import print_function # Python 2/3 compatibility
import os
import json
import boto3
import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
from dynamodb import Dynamodb
import decimal


def build(item, result_list):
    account_id = item['account_id']
    product_code = item['product_code']
    date_time = item['datetime']
    blended = item['blended']
    unblended = item['unblended']
    usage_amount = item['usage_amount']
    max_val = item.get('yhat_upper_exp')
    if account_id not in result_list:
        result_list[account_id] = {}
    if product_code not in result_list[account_id]:
        result_list[account_id][product_code] = {'datetime': date_time, 'blended': blended, 'unblended': unblended, 'max_val': max_val, 'usage_amount': usage_amount}
    else:
        if result_list[account_id][product_code]['datetime'] < date_time:
            result_list[account_id][product_code] = {'datetime': date_time, 'blended': blended, 'unblended': unblended, 'max_val': max_val, 'usage_amount': usage_amount}


region = os.environ.get('AWS_DEFAULT_REGION')
table_name = os.environ.get('DYNAMODB_TABLE_NAME')
compare_table_name = os.environ.get('DYNAMODB_COMPARE_TABLE_NAME')
client = boto3.resource('dynamodb', region_name=region)
table = client.Table(table_name)
dynamodb = Dynamodb(client, compare_table_name)

current_date = datetime.datetime.utcnow()
prev_date = current_date + relativedelta(days=-1)
#prev_date = current_date
next_date = current_date + relativedelta(days=1)

print("\n+++++finding items with no max")
# find items with NO predictions
response = table.scan(
    FilterExpression="attribute_not_exists(yhat) and attribute_exists(blended) and #start >= :start and #end < :end",
    ExpressionAttributeNames={'#start': 'datetime', '#end': 'datetime'},
    ExpressionAttributeValues={':start': prev_date.strftime('%Y-%m-%d'), ':end': next_date.strftime('%Y-%m-%d')}
)
print(len(response['Items']))

no_predictions = {}
for item in response['Items']:
    build(item, no_predictions)

print(len(no_predictions))
for account_id in no_predictions.keys():
    product_codes = ""
    for product_code in no_predictions[account_id]:
        if product_codes != "":
            product_codes += ", "
        product_codes += product_code
    print('%s - %s' % (account_id, product_codes))
