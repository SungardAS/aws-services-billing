
import boto3
client = boto3.client('cloudwatch')

def find_account_metrics(next_token):
    kwargs = {
        'Namespace': 'AWS/Billing',
        'MetricName': 'EstimatedCharges',
        'Dimensions': [
            {
                'Name': 'LinkedAccount',
                #'Value': 'string'
            }
        ]
    }
    if next_token:
        kwargs['NextToken'] = next_token
    response = client.list_metrics(**kwargs)
    print('next token = %s' % response.get('NextToken'))
    return response

def find_service_metrics(account_id, next_token):
    kwargs = {
        'Namespace': 'AWS/Billing',
        'MetricName': 'EstimatedCharges',
        'Dimensions': [
            {
                'Name': 'LinkedAccount',
                'Value': account_id
            },
            {
                'Name': 'ServiceName',
                #'Value': 'string'
            }
        ]
    }
    if next_token:
        kwargs['NextToken'] = next_token
    response = client.list_metrics(**kwargs)
    print('next token = %s' % response.get('NextToken'))
    return response

def find_accounts():
    accounts = []
    next_token = None
    while True:
        response = find_account_metrics(next_token)
        for metric in response['Metrics']:
            for dimension in metric['Dimensions']:
                if dimension['Name'] == "LinkedAccount" and dimension['Value'] not in accounts:
                    accounts.append(dimension['Value'])
        #print accounts
        next_token = response.get('NextToken')
        if not next_token:  break
    return accounts

def find_services(account_id):
    services = []
    next_token = None
    while True:
        response = find_service_metrics(account_id, next_token)
        for metric in response['Metrics']:
            for dimension in metric['Dimensions']:
                if dimension['Name'] == "ServiceName" and dimension['Value'] not in services:
                    services.append(dimension['Value'])
        #print services
        next_token = response.get('NextToken')
        if not next_token:  break
    return services


if __name__ == "__main__":
    accounts = find_accounts()
    print accounts
