
from __future__ import print_function # Python 2/3 compatibility

import sys
import os
import json
import boto3
import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
from dynamodb import Dynamodb
import decimal

import psycopg2

def find_charge(cur, account_id, product_code, current_date):
    sql = "select lineItem_UsageAccountId, lineitem_productcode,"
    sql += " max(lineitem_usageenddate),"
    sql += " sum(lineItem_UsageAmount),"
    sql += " to_char(sum(cast(lineItem_BlendedCost as float)), 'FM999,999,999,990D00'),"
    sql += " to_char(sum(cast(lineitem_unblendedcost as float)), 'FM999,999,999,990D00')"
    sql += " from AWSBilling%s" % (current_date.strftime('%Y%m'))
    sql += " where lineitem_productcode = '%s'" % (product_code)
    sql += " and lineitem_lineitemtype = 'Usage'"
    sql += " and lineitem_usageaccountid = '%s'" % (account_id)
    sql += " and lineitem_usageenddate <= '%s'" % (current_date.strftime('%Y-%m-%dT%H:%M:%SZ'))
    sql += " group by lineItem_UsageAccountId, lineitem_productcode"
    sql += " order by lineItem_UsageAccountId, lineitem_productcode"

    cur.execute(sql)
    return cur.fetchall()

def compare(cur, current_date, account_id, product_code):

    current_date_str = current_date.strftime('%Y-%m-%d %H:%M:%S')
    print(current_date_str)

    rows = find_charge(cur, account_id, product_code, current_date)
    if len(rows) == 0:
        print("no charge data found")
        return None

    current_row = rows[0]
    print(current_row)
    print("%s,%s,%s,%s,%s,%s\n" % (current_row[0], current_row[1], current_row[2], current_row[3], current_row[4], current_row[5]))
    current_blended = float(current_row[4].replace(',', ''))
    current_unblended = float(current_row[5].replace(',', ''))
    current_date_str = parser.parse(current_row[2]).strftime('%Y-%m-%d %H:%M:%S')

    compared = {
        'account_id': account_id,
        'product_code': product_code,
        'blended': [{'datetime': current_date_str, 'charge': current_blended, 'increased': 0}],
        'unblended': [{'datetime': current_date_str, 'charge': current_unblended, 'increased': 0}]
    }

    min_prev_date = '2016-08-01'
    max_loop_count = 5

    prev_date = parser.parse(current_row[2]) + relativedelta(months=-1)
    prev_date_str = prev_date.strftime('%Y-%m-%d %H:%M:%S')
    print("\n%s"% prev_date_str)
    loop_count = 0

    while prev_date_str > min_prev_date and loop_count < max_loop_count:
    #while prev_date_str > min_prev_date:

        prev_rows = find_charge(cur, account_id, product_code, prev_date)
        if len(prev_rows) == 0:
            break
        prev_row = prev_rows[0]
        print("%s,%s,%s,%s,%s,%s" % (prev_row[0], prev_row[1], prev_row[2], prev_row[3], prev_row[4], prev_row[5]))
        prev_blended = float(prev_row[4].replace(',', ''))
        prev_unblended = float(prev_row[5].replace(',', ''))

        if prev_blended > 0:
            blended_increased = round(((current_blended - prev_blended) / prev_blended) * 100, 2)
        else:
            blended_increased = None
        if prev_unblended > 0:
            unblended_increased = round(((current_unblended - prev_unblended) / prev_unblended) * 100, 2)
        else:
            unblended_increased = None
        print('increased blended:\t%s%%\t%s --> %s' % (blended_increased, prev_blended, current_blended))
        print('increased unblended:\t%s%%\t%s --> %s' % (unblended_increased, prev_unblended, current_unblended))

        compared['blended'].append({'datetime': prev_date_str, 'increased': blended_increased, 'charge': prev_blended})
        compared['unblended'].append({'datetime': prev_date_str, 'increased': unblended_increased, 'charge': prev_unblended})

        loop_count += 1
        print('loop count = %d' % loop_count)

        prev_date = prev_date + relativedelta(months=-1)
        prev_date_str = prev_date.strftime('%Y-%m-%d %H:%M:%S')
        print("\n%s"% prev_date_str)

    return compared


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
        #result_list[account_id][product_code] = [{'datetime': date_time, 'blended': blended, 'unblended': unblended, 'max': max_val}]
        result_list[account_id][product_code] = {'datetime': date_time, 'blended': blended, 'unblended': unblended, 'max_val': max_val, 'usage_amount': usage_amount}
    else:
        #found = [t['datetime'] for t in result_list[account_id][product_code] if t['datetime'] == date_time]
        #if len(found) == 0:
        #    result_list[account_id][product_code].append({'datetime': date_time, 'blended': blended, 'unblended': unblended, 'max': max_val})
        if result_list[account_id][product_code]['datetime'] < date_time:
            result_list[account_id][product_code] = {'datetime': date_time, 'blended': blended, 'unblended': unblended, 'max_val': max_val, 'usage_amount': usage_amount}


def save(current_date, result_list, dynamodb, cur):
    for account_id in result_list.keys():
        print(account_id)
        for product_code in result_list[account_id]:
            print(product_code)
            result = result_list[account_id][product_code]
            print(result)
            item = {
                'id': '%s_%s_%s' % (account_id, product_code, parser.parse(result['datetime']).strftime("%Y-%m")),
                'account_id': account_id,
                'product_code': product_code,
                'datetime': '%s' % result['datetime'],
                'time_stamp': parser.parse(result['datetime']).strftime("%s"),
                #'blended': result['blended'],
                #'unblended': result['unblended'],
                #'usage_amount': result['usage_amount']
            }
            #if result.get('max_val'):
            #    item['max_val'] = result['max_val']

            compared = compare(cur, current_date, account_id, product_code)
            if compared is None:
                continue

            item['compared'] = json.dumps(compared)

            blended_cur_total = compared['blended'][0]['charge']
            unblended_cur_total = compared['unblended'][0]['charge']
            item['blended_ct'] = decimal.Decimal('%.2f' % blended_cur_total)
            item['unblended_ct'] = decimal.Decimal('%.2f' % unblended_cur_total)

            """blended_prev_total = 0
            unblended_prev_total = 0
            for index, prev in enumerate(compared['blended']):
                if index == 0:  continue
                blended_prev_total += prev['charge']
            for index, prev in enumerate(compared['unblended']):
                if index == 0:  continue
                unblended_prev_total += prev['charge']
            if len(compared['blended']) > 1:
                blended_prev_avg = blended_prev_total / (len(compared['blended']) - 1)
                unblended_prev_avg = unblended_prev_total / (len(compared['unblended']) - 1)
            else:
                blended_prev_avg = 0
                unblended_prev_avg = 0
            if blended_prev_avg > 0:
                blended_increased = ((blended_cur_total - blended_prev_avg) / blended_prev_avg) * 100
                item['total_blended_diff_increased'] = decimal.Decimal('%.2f' % blended_increased)
            if unblended_prev_avg > 0:
                unblended_increased = ((unblended_cur_total - unblended_prev_avg) / unblended_prev_avg) * 100
                item['total_unblended_diff_increased'] = decimal.Decimal('%.2f' % unblended_increased)
            item['total_blended_diff'] = decimal.Decimal('%.2f' % (blended_cur_total - blended_prev_avg))
            item['total_unblended_diff'] = decimal.Decimal('%.2f' % (unblended_cur_total - unblended_prev_avg))"""

            # extract prev totals
            blended_prev_totals = [prev['charge'] for prev in compared['blended']]
            del blended_prev_totals[0]  # remove the current total
            #blended_prev_totals.sort()
            unblended_prev_totals = [prev['charge'] for prev in compared['unblended']]
            del unblended_prev_totals[0]  # remove the current total
            #unblended_prev_totals.sort()

            # calculate diffs
            blended_total_diffs = [blended_cur_total-prev for prev in blended_prev_totals]
            unblended_total_diffs = [unblended_cur_total-prev for prev in unblended_prev_totals]

            # extract prev total increased
            blended_total_increased_diffs = [prev['increased'] for prev in compared['blended']]
            del blended_total_increased_diffs[0]  # remove the current total
            #blended_total_increased_diffs = [prev for prev in blended_total_increased_diffs if prev]    # remove None
            #blended_total_increased_diffs.sort()
            unblended_total_increased_diffs = [prev['increased'] for prev in compared['unblended']]
            del unblended_total_increased_diffs[0]  # remove the current total
            #unblended_total_increased_diffs = [prev for prev in unblended_total_increased_diffs if prev]    # remove None
            #unblended_total_increased_diffs.sort()

            """if len(blended_total_diffs) >= 2:
                item['blended_nt_diff'] = decimal.Decimal('%.2f' % blended_total_diffs[0])
                item['blended_xt_diff'] = decimal.Decimal('%.2f' % blended_total_diffs[len(blended_total_diffs)-1])
            elif len(blended_total_diffs) == 1:
                item['blended_xt_diff'] = decimal.Decimal('%.2f' % blended_total_diffs[0])

            if len(unblended_total_diffs) >= 2:
                item['unblended_nt_diff'] = decimal.Decimal('%.2f' % unblended_total_diffs[0])
                item['unblended_xt_diff'] = decimal.Decimal('%.2f' % unblended_total_diffs[len(unblended_total_diffs)-1])
            elif len(unblended_total_diffs) == 1:
                item['unblended_xt_diff'] = decimal.Decimal('%.2f' % unblended_total_diffs[0])

            if len(blended_total_increased_diffs) >= 2:
                item['blended_xti_diff'] = decimal.Decimal('%.2f' % blended_total_increased_diffs[0])
                item['blended_nti_diff'] = decimal.Decimal('%.2f' % blended_total_increased_diffs[len(blended_total_increased_diffs)-1])
            elif len(blended_total_increased_diffs) == 1:
                item['blended_xti_diff'] = decimal.Decimal('%.2f' % blended_total_increased_diffs[0])

            if len(unblended_total_increased_diffs) >= 2:
                item['unblended_xti_diff'] = decimal.Decimal('%.2f' % unblended_total_increased_diffs[0])
                item['unblended_nti_diff'] = decimal.Decimal('%.2f' % unblended_total_increased_diffs[len(unblended_total_increased_diffs)-1])
            elif len(unblended_total_increased_diffs) == 1:
                item['unblended_xti_diff'] = decimal.Decimal('%.2f' % unblended_total_increased_diffs[0])"""

            if len(blended_total_diffs) >= 2:
                item['blended_diff_1'] = decimal.Decimal('%.2f' % blended_total_diffs[0])
                item['blended_diff_2'] = decimal.Decimal('%.2f' % blended_total_diffs[1])
            elif len(blended_total_diffs) == 1:
                item['blended_diff_1'] = decimal.Decimal('%.2f' % blended_total_diffs[0])

            if len(unblended_total_diffs) >= 2:
                item['unblended_diff_1'] = decimal.Decimal('%.2f' % unblended_total_diffs[0])
                item['unblended_diff_2'] = decimal.Decimal('%.2f' % unblended_total_diffs[1])
            elif len(unblended_total_diffs) == 1:
                item['unblended_diff_1'] = decimal.Decimal('%.2f' % unblended_total_diffs[0])

            if len(blended_total_increased_diffs) >= 2:
                if blended_total_increased_diffs[0]:   item['blended_inc_diff_1'] = decimal.Decimal('%.2f' % blended_total_increased_diffs[0])
                if blended_total_increased_diffs[1]:   item['blended_inc_diff_2'] = decimal.Decimal('%.2f' % blended_total_increased_diffs[1])
            elif len(blended_total_increased_diffs) == 1:
                if blended_total_increased_diffs[0]:    item['blended_inc_diff_1'] = decimal.Decimal('%.2f' % blended_total_increased_diffs[0])

            if len(unblended_total_increased_diffs) >= 2:
                if unblended_total_increased_diffs[0]:  item['unblended_inc_diff_1'] = decimal.Decimal('%.2f' % unblended_total_increased_diffs[0])
                if unblended_total_increased_diffs[1]:  item['unblended_inc_diff_2'] = decimal.Decimal('%.2f' % unblended_total_increased_diffs[1])
            elif len(unblended_total_increased_diffs) == 1:
                if unblended_total_increased_diffs[0]:  item['unblended_inc_diff_1'] = decimal.Decimal('%.2f' % unblended_total_increased_diffs[0])

            print(item)
            dynamodb.create(item)


"""def save(new_item, dynamodb):

    account_id = new_item['account_id']
    product_code = new_item['product_code']
    date_time = new_item['datetime']
    max_val = new_item.get('yhat_upper_exp')
    blended = new_item.get('blended')
    unblended = new_item.get('unblended')
    usage_amount = new_item.get('usage_amount')

    id = '%s_%s' % (account_id, product_code)
    print('id: %s' % id)
    '''item = dynamodb.find_by_id(id)
    if item:
        print('item found %s' % item)
        if item['datetime'] >= date_time:
            print("no need to save because the datetime of new item is before the one of the saved")
            return
        item['usage_amount'] = row[3]
        item['blended'] = blended
        item['unblended'] = unblended
        dynamodb.update(item)
    else:
        item = {
            'id': '%s_%s' % (row[1], row[2]),
            'account_id': row[1],
            'product_code': row[2],
            'datetime': '%s' % row[0],
            'usage_amount': row[3],
            'blended': blended,
            'unblended': unblended,
            'time_stamp': row[0].strftime("%s")
        }
        print('add a new item %s' % item)
        dynamodb.create(item)'''
    item = {
        'id': '%s_%s' % (row[1], row[2]),
        'account_id': row[1],
        'product_code': row[2],
        'datetime': '%s' % row[0],
        'time_stamp': row[0].strftime("%s")
    }
    if max_val:
        item['max_val'] = max_val
    if blended:
        item['blended'] = blended
    if unblended:
        item['unblended'] = unblended
    if usage_amount:
        item['usage_amount'] = usage_amount
    dynamodb.create(item)
"""

if __name__ == "__main__":

    host = os.environ.get('REDSHIFT_HOST_NAME')
    dbname = os.environ.get('REDSHIFT_DATABSE_NAME')
    port = os.environ.get('REDSHIFT_DATABSE_PORT')
    user = os.environ.get('REDSHIFT_USER_NAME')
    pwd = os.environ.get('REDSHIFT_PASSWORD')

    con = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=pwd)
    cur = con.cursor()

    region = os.environ.get('AWS_DEFAULT_REGION')
    table_name = os.environ.get('DYNAMODB_TABLE_NAME')
    compare_table_name = os.environ.get('DYNAMODB_COMPARE_TABLE_NAME')
    client = boto3.resource('dynamodb', region_name=region)
    table = client.Table(table_name)
    dynamodb = Dynamodb(client, compare_table_name)

    if len(sys.argv) == 1:
        current_date = datetime.datetime.utcnow()
    else:
        current_date = parser.parse(sys.argv[1])
        current_date = datetime.datetime(current_date.year, current_date.month, 1)
        current_date = current_date + relativedelta(months=1) + relativedelta(days=-1)
    print("cuurent_date = %s" % current_date)

    #current_date = datetime.datetime.utcnow()
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
    #print(no_predictions)
    save(current_date, no_predictions, dynamodb, cur)

    # find items whos has both predictions and blended/unblended
    print("\n\n+++++finding items with spikes")
    """ScanFilter={
        'yhat': {'ComparisonOperator': 'NOT_NULL'},
        'blended': {'ComparisonOperator': 'NOT_NULL'}
        'datetime': {
            'AttributeValueList': [{'S': prev_date.strftime('%Y-%m-%d')}],
            'ComparisonOperator': 'GE'
        }
        'datetime': {
            'AttributeValueList': [{'S': current_date.strftime('%Y-%m-%d')}],
            'ComparisonOperator': 'LE'
        }
    },"""
    response = table.scan(
        FilterExpression="attribute_exists(yhat) and attribute_exists(blended) and #start >= :start and #end < :end",
        ExpressionAttributeNames={'#start': 'datetime', '#end': 'datetime'},
        ExpressionAttributeValues={':start': prev_date.strftime('%Y-%m-%d'), ':end': next_date.strftime('%Y-%m-%d')}
    )
    print(len(response['Items']))
    spike_compares = {}
    for item in response['Items']:
        #if item['yhat_upper_exp'] < item['blended'] or item['yhat_upper_exp'] < item['unblended']:
        if item['yhat_exp'] < item['unblended']:
            #print("spike found: %s\t%s\t%s\t%s\t%s\t%s" % (item['account_id'], item['product_code'], item['datetime'], item['blended'], item['unblended'], item['yhat_upper_exp']))
            build(item, spike_compares)
    #print(spike_compares)
    save(current_date, spike_compares, dynamodb, cur)


    cur.close()
    con.close()
