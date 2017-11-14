
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
spike_table_name = os.environ.get('DYNAMODB_SPIKE_TABLE_NAME')
client = boto3.resource('dynamodb', region_name=region)
spike_table = client.Table(spike_table_name)


def lambda_handler(event, context):

    given_date = event.get('date')
    current_date = datetime.datetime.utcnow()
    if given_date:
        given_date = parser.parse(given_date)
        if given_date.year != current_date.year or given_date.month != current_date.month:
            current_date = datetime.datetime(given_date.year, given_date.month, 1) + relativedelta(months=1) + relativedelta(days=-1)

    report_type = event['type']
    if report_type == 'summary':
        return get_summary_message(current_date)
    else:
        return get_detail_messages(current_date, event['account_id'])
    raise "Not supported type, %s" % report_type


def get_summary_message(current_date):

    if current_date.day > 1:
        from_date = current_date + relativedelta(days=-1)
        #from_date = datetime.datetime(current_date.year, current_date.month, 1)
    else:
        from_date = current_date
    to_date = current_date + relativedelta(days=1)
    print "from_date = %s, to_date = %s" % (from_date, to_date)

    response = spike_table.scan(
        FilterExpression="#start >= :start and #end < :end",
        ExpressionAttributeNames={
            '#start': 'datetime',
            '#end': 'datetime'
        },
        ExpressionAttributeValues={
            ':start': from_date.strftime('%Y-%m-%d'),
            ':end': to_date.strftime('%Y-%m-%d')
        }
    )
    #print("there are %d items found" % len(response['Items']))

    result = {}
    for item in response['Items']:
        account_id = item['account_id']
        product_code = item['product_code']
        if account_id not in result:
            result[account_id] = {}
        result[account_id][product_code] = item

    return result


def get_detail_messages(current_date, account_id):

    if current_date.day > 1:
        from_date = current_date + relativedelta(days=-1)
        #from_date = datetime.datetime(current_date.year, current_date.month, 1)
    else:
        from_date = current_date
    to_date = current_date + relativedelta(days=1)
    print "from_date = %s, to_date = %s" % (from_date, to_date)

    response = spike_table.scan(
        FilterExpression="#account_id = :account_id and #start >= :start and #end < :end",
        ExpressionAttributeNames={
            '#account_id': 'account_id',
            '#start': 'datetime',
            '#end': 'datetime'
        },
        ExpressionAttributeValues={
            ':account_id': account_id,
            ':start': from_date.strftime('%Y-%m-%d'),
            ':end': to_date.strftime('%Y-%m-%d')
        }
    )
    #print("there are %d items found" % len(response['Items']))

    messages = []

    for item in response['Items']:

        from_zone = tz.gettz('UTC')
        utc = datetime.datetime.strptime(item["blended"][0]['datetime'], '%Y-%m-%d %H:%M:%S')
        utc = utc.replace(tzinfo=from_zone)

        message = "As of %s, the billing amount of '%s' in Account, '%s(%s)', is $%s" % (utc, item['product_code'], item['account_id'], item['account_name'], item["blended"][0]['charge'])
        if len(item["blended"]) >= 2:
            if item["blended"][1].get('increased'):
                message += ", which is %.2f%% increase from last month ($%s)" % (decimal.Decimal(item["blended"][1]['increased']), item["blended"][1]['charge'])
            else:
                message += " which is increase from last month ($%s)" % (item["blended"][1]['charge'])
        if len(item["blended"]) >= 3:
            if item["blended"][2].get('increased'):
                message += " and %.2f%% increase from 2 months ago ($%s)" % (decimal.Decimal(item["blended"][2]['increased']), item["blended"][2]['charge'])
            else:
                message += " and increase from 2 months ago ($%s)" % (item["blended"][2]['charge'])
        message += ". Please see this graph for more detail, %s" % (item['image_file_path'])

        messages.append(message)

    return messages
