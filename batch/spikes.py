
import sys
import os
import boto3
import json

import decimal
import datetime
from dateutil.relativedelta import relativedelta
from dateutil import parser
from dateutil import tz
from dynamodb import Dynamodb

import matplotlib
matplotlib.use('Agg')

region = os.environ.get('AWS_DEFAULT_REGION')
compare_table_name = os.environ.get('DYNAMODB_COMPARE_TABLE_NAME')
spike_table_name = os.environ.get('DYNAMODB_SPIKE_TABLE_NAME')
s3_graph_bucket = os.environ.get('S3_GRAPH_BUCKET')
client = boto3.resource('dynamodb', region_name=region)
compare_table = client.Table(compare_table_name)
spike_table = Dynamodb(client, spike_table_name)
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
org = boto3.client('organizations')


def autolabel(ax, rects):
    """
    Attach a text label above each bar displaying its height
    """
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '%.2f' % height,
                ha='center', va='bottom')


def draw_bar(account_id, product_code, compared):

    import numpy as np
    import matplotlib.pyplot as plt

    #given_time = parser.parse(compared['unblended'][0]['datetime']).strftime('%H:%M')

    fig = plt.gcf()

    x = [parser.parse(v['datetime']).strftime('%Y-%m-%d') for v in compared['unblended']][::-1]
    y = [decimal.Decimal(v['charge']) for v in compared['unblended']][::-1]

    n_groups = len(x)

    fig, ax = plt.subplots()
    fig.set_figwidth(10)

    index = np.arange(n_groups)
    bar_width = 0.35

    opacity = 0.4
    #error_config = {'ecolor': '0.3'}

    rects = plt.bar(index, y, bar_width,
                     alpha=opacity,
                     color='b')
                     #yerr=std_men,
                     #error_kw=error_config,
                     #label='Men')

    plt.title('%s in %s\n' % (product_code, account_id))
    #account_name = get_account_info(account_id)
    #plt.title('%s in %s (%s)\n' % (product_code, account_id, account_name))
    #plt.xlabel('As of %s' % given_time)
    plt.xlabel('As of %s UTC' % (parser.parse(compared['unblended'][0]['datetime']).strftime('%H:%M')))
    #plt.ylabel('Scores')
    #plt.title('Scores by group and gender')
    plt.xticks(index + bar_width / 2, x)
    plt.legend()

    plt.tight_layout()

    autolabel(ax, rects)

    plt.show()

    #ax = plt.subplot()
    #ax.bar(x, y, width=10)
    #ax.xaxis_date()

    #plt.show()

    image_file_path = 'graphs/%s_%s_%s.png' % (compared['account_id'], compared['product_code'], compared['unblended'][0]['datetime'].replace(' ', 'A').replace(':', ''))
    #print('image file = %s' % image_file_path)
    fig.savefig(image_file_path, dpi=100)

    s3.upload_file(image_file_path, s3_graph_bucket, image_file_path)
    object_acl = s3_resource.ObjectAcl(s3_graph_bucket, image_file_path)
    object_acl.put(ACL='public-read')
    return image_file_path


def get_account_info(account_id):
    response = org.describe_account(AccountId=account_id)
    return response['Account']


def get_summary_message(result):
    message = "There are %d accounts with spikes and they are " % (len(result.keys()))
    for account_id in result.keys():
        message += "\n%s(%s) - " % (account_id, get_account_info(account_id)['Name'])
        product_message = ""
        for product_code in result[account_id].keys():
            if product_message != "":
                product_message += ", "
            product_message += "%s" % product_code
        message += product_message
    return message


def get_detail_message(account_id, product_code):

    compared = result[account_id][product_code]

    #image_file_path = draw_bar(account_id, product_code, compared)

    from_zone = tz.gettz('UTC')
    utc = datetime.datetime.strptime(compared["blended"][0]['datetime'], '%Y-%m-%d %H:%M:%S')
    utc = utc.replace(tzinfo=from_zone)

    message = "As of %s, the billing amount of '%s' in Account, '%s(%s)', is $%s" % (utc, product_code, account_id, get_account_info(account_id)['Name'], compared["blended"][0]['charge'])
    if len(compared["blended"]) >= 2:
        if compared["blended"][1].get('increased'):
            message += ", which is %.2f%% increase from last month ($%s)" % (decimal.Decimal(compared["blended"][1]['increased']), compared["blended"][1]['charge'])
        else:
            message += " which is increase from last month ($%s)" % (compared["blended"][1]['charge'])
    if len(compared["blended"]) >= 3:
        if compared["blended"][2].get('increased'):
            message += " and %.2f%% increase from 2 months ago ($%s)" % (decimal.Decimal(compared["blended"][2]['increased']), compared["blended"][2]['charge'])
        else:
            message += " and increase from 2 months ago ($%s)" % (compared["blended"][2]['charge'])
    #message += ". Please see this graph for more detail, https://s3.amazonaws.com/%s/%s" % (s3_graph_bucket, image_file_path)
    message += ". Please see this graph for more detail, %s" % (compared['image_file_path'])
    return message


if __name__ == "__main__":

    if len(sys.argv) == 1:
        current_date = datetime.datetime.utcnow()
    else:
        current_date = parser.parse(sys.argv[1])
        current_date = datetime.datetime(current_date.year, current_date.month, 1)
        current_date = current_date + relativedelta(months=1) + relativedelta(days=-1)
    print("cuurent_date = %s" % current_date)

    #current_date = datetime.datetime.utcnow()
    #from_date = datetime.datetime(current_date.year, current_date.month, 1)
    from_date = current_date + relativedelta(days=-1)
    to_date = current_date + relativedelta(days=1)
    min_amount_per_day = 2
    min_amount = min_amount_per_day * current_date.day
    min_increased = 10

    response = compare_table.scan(
        FilterExpression="(#unblended_diff_1 > :min_amount or #unblended_diff_2 > :min_amount) and (#unblended_inc_diff_1 > :min_increased or #unblended_inc_diff_2 > :min_increased) and #start >= :start and #end < :end",
        ExpressionAttributeNames={
          '#unblended_diff_1': 'unblended_diff_1',
          '#unblended_diff_2': 'unblended_diff_2',
          '#unblended_inc_diff_1': 'unblended_inc_diff_1',
          '#unblended_inc_diff_2': 'unblended_inc_diff_2',
          '#start': 'datetime',
          '#end': 'datetime'
        },
        ExpressionAttributeValues={
          ':min_amount': min_amount,
          ':min_increased': min_increased,
          ':start': from_date.strftime('%Y-%m-%d'),
          ':end': to_date.strftime('%Y-%m-%d')
        }
    )
    #print("there are %d items found" % len(response['Items']))

    result = {}
    for item in response['Items']:
        account_id = item['account_id']
        product_code = item['product_code']
        compared = json.loads(item['compared'])
        # change the float to decimal
        for blended in compared['blended']:
            blended['charge'] = decimal.Decimal('%.2f' % blended['charge'])
            if blended['increased']:
                blended['increased'] = decimal.Decimal('%.2f' % blended['increased'])
        for unblended in compared['unblended']:
            unblended['charge'] = decimal.Decimal('%.2f' % unblended['charge'])
            if unblended['increased']:
                unblended['increased'] = decimal.Decimal('%.2f' % unblended['increased'])
        compared['id'] = item['id']
        compared['account_name'] = get_account_info(account_id)['Name']
        compared['datetime'] = item['datetime']
        image_file_path = draw_bar(account_id, product_code, compared)
        compared['image_file_path'] = "https://s3.amazonaws.com/%s/%s" % (s3_graph_bucket, image_file_path)
        print('%s' % compared)
        spike_table.create(compared)
        if account_id not in result:
            result[account_id] = {}
        result[account_id][product_code] = compared

    print("\n%s" % get_summary_message(result))

    for account_id in result.keys():
        #print("account_id : %s" % account_id)
        for product_code in result[account_id].keys():
            #print("\tproduct_code : %s" % product_code)
            print("\n%s" % get_detail_message(account_id, product_code))
