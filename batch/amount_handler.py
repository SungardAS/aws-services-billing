
######################################################################################################################
# This is a lambda function to provide "billing spikes" information stored in billing dynamodb table, 'billing_spikes'
######################################################################################################################

import os
import boto3
import json

import decimal
import datetime
from dateutil.relativedelta import relativedelta
from dateutil import tz
from dateutil import parser

region = os.environ.get('AWS_DEFAULT_REGION')
amount_table_name = os.environ.get('DYNAMODB_AGGR_TABLE_NAME')
client = boto3.resource('dynamodb', region_name=region)
amount_table = client.Table(amount_table_name)


def lambda_handler(event, context):

    given_date = event.get('date')
    current_date = datetime.datetime.utcnow()
    if given_date:
        given_date = parser.parse(given_date)
        if given_date.year != current_date.year or given_date.month != current_date.month:
            current_date = datetime.datetime(given_date.year, given_date.month, 1) + relativedelta(months=1) + relativedelta(days=-1)
    print("current_date = %s" % current_date)

    report_type = event['type']
    if report_type == 'summary':
        return get_summary_message(current_date)
    else:
        return get_detail_messages(current_date, event['account_id'])
    raise "Not supported type, %s" % report_type


def get_summary_message(current_date):

    response = amount_table.query(
        KeyConditionExpression="id = :id",
        ExpressionAttributeValues={
            ':id': '*_%s' % current_date.strftime('%Y-%m')
        }
    )
    #print("there are %d items found" % len(response['Items']))

    return response['Items']


def get_detail_messages(current_date, account_id):

    response = amount_table.query(
        KeyConditionExpression="id = :id",
        ExpressionAttributeValues={
            ':id': '%s_%s' % (account_id, current_date.strftime('%Y-%m'))
        }
    )
    #print("there are %d items found" % len(response['Items']))

    return response['Items']
