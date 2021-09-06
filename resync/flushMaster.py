import requests 
import datetime
import json
import pymysql
import boto3
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/..') # Can setup in lambda layer instead.
from GroupSchedule import OfferGroupsCursor, api_req, api_url, config_params
from util.database import sql_singleton


database = sql_singleton() # connect mysql server
worker_arn = os.environ['worker_arn']

lambda_resource = boto3.client('lambda', region_name=os.environ['lambda_region'])

def lambda_handler(even, config) :

    # Select every task that has meet the resync condition (sub_id > 0, status=='close', current - sub_id > actived_to)
    cursor = database.cursor(pymysql.cursors.DictCursor)
    current_t = datetime.datetime.now() + datetime.timedelta(hours=8)
    cursor.execute('''SELECT DISTINCT created_at FROM GroupSchedule 
    WHERE follow = 0 
    AND sub_1 > 0 
    AND status = 'close'
    AND DATE_ADD(actived_to, INTERVAL sub_1 HOUR) <= '%s'
    LIMIT 5 
    '''%current_t)
    tasks = cursor.fetchall()
    for t in tasks :

        cursor.execute('''SELECT * FROM GroupSchedule 
        WHERE sub_1 > 0 
        AND created_at = '%s' '''%t['created_at'])
        task = cursor.fetchall()

        # Get payout type of current resync target, to know how to adjust the payout (CPA/ CPS/ Both).
        q = api_req(api_url, [
            {'Target' : 'Offer'},
            {'Method' : 'findById'},
            {'id' : task[0]['offer_id']},
            {'fields[]' : 'payout_type'},
            config_params
        ])
        if not q.json()['response']['errors'] :
            ptype = q.json()['response']['data']['Offer']['payout_type']
        else :
            raise ValueError(q.json()['response']['errors'])
        output = {
            'offer_id' : task[0]['offer_id'],
            'actived_from' : task[0]['actived_from'],
            'actived_to' : task[0]['actived_to'],
            'sub_1' : task[0]['sub_1'],
            'ptype' : ptype,
            'cashflow_group_data' : [ {'cashflow_group_id' : x['cashflow_group_id'], 'percent' : x['percent'], 'rate' : x['rate'] }
                for x in task if x['cashflow_group_id']
            ]
        }
        print('Start Flushing Offer : %s'%task[0]['offer_id'])
        flushMaster(output)
        cursor.execute('''UPDATE GroupSchedule
        SET sub_1 = 0 
        WHERE created_at = '%s' '''%t['created_at'])
    
def flushMaster(payload) :
    
    # Get the resync interval from sub_1, which is also the input of `force_resync_at` in the sheet.
    payload['sub_1'] = datetime.timedelta(hours=int(payload['sub_1']))
    diff = payload['sub_1'] if payload['actived_to'] - payload['actived_from'] > payload['sub_1'] else payload['actived_to'] - payload['actived_from']


    # actived_from - interval, actived_to - interval will be load as OfferGroupsCursor objects to represent the correct reference for resync.
    for d in [payload['actived_from'], payload['actived_to']] :
        cursor = OfferGroupsCursor(payload['offer_id'], branch_at=(d-diff).strftime('%Y-%m-%d %H:%M:%S'))
        context = []
        for cashflow_group_data in payload['cashflow_group_data'] :
            cashflow_group_id = cashflow_group_data['cashflow_group_id']
            context.append({'cashflow_group_id' : cashflow_group_id, 'percent' : 0, 'rate' : 0})
            rate = cursor.groups[cashflow_group_id].rate
            rate = rate if rate else 0
            percent = cursor.groups[cashflow_group_id].percent
            percent = percent if percent else 0
            if payload['ptype'] == 'cpa_percentage' : 
                context[-1]['percent'] = float(percent)
            elif payload['ptype'] == 'cpa_flat' :
                context[-1]['rate'] = float(rate)
            else :
                context[-1]['rate'] = float(rate)
                context[-1]['percent'] = float(percent)
        
        # Payload with correct reference and period will be sent to worker for adjustment.
        invoke_payload = {
            'offer_id' : payload['offer_id'],
            'ptype' : payload['ptype'],
            'default_value' : cursor.default_value,
            'from' : (d-diff).strftime('%Y-%m-%d %H:%M:%S'), 
            'to' : d.strftime('%Y-%m-%d %H:%M:%S'),
            'context' : context
        }
        lambda_resource.invoke(
            FunctionName = worker_arn,
            InvocationType = 'Event',
            Payload = json.dumps(invoke_payload)
        )
        print(invoke_payload)


def api_req(url, key_value_pair) :
    params_list = [list(x)[0] + '=' + x[list(x)[0]] for x in key_value_pair]
    url = url + '?' + '&'.join(params_list)
    response = requests.get(url)
    return response