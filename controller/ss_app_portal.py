import pymysql
import time
import os
import sys 

# this function should be implemented on lambda which integrated with API Gateway where spreadhsheet is sending data to.

sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/..') # Can setup in lambda layer instead.

from GroupSchedule import OfferGroupsCursor, api_req, api_url, config_params
from util.database import sql_singleton



database = sql_singleton() # connect mysql server


def ss_app_portal(even, context) : 

    # load payload sent from spreadsheet.
    print(even)
    response = []
    offer_id = even[0]['offer_id']
    actived_from = even[0]['actived_from']
    
    # check if baseline of this offer_id exist in database or not, if not, save one with online configuration.
    db_cursor = database.cursor(pymysql.cursors.DictCursor)
    db_cursor.execute('''SELECT * FROM GroupSchedule WHERE offer_id = '%s' and group_type = 'base' '''%offer_id)
    base = db_cursor.fetchall()
    if len(base) == 0 : 
        base_cursor = OfferGroupsCursor(offer_id)
        base_cursor.setup_period(is_base=True)
        base_cursor.push_cursor()
        time.sleep(1) 
    

    # create OfferGroupCursor with `branch_at = actived_to`, to have all the execution base on that specific snapshot.
    cursor = OfferGroupsCursor(offer_id, branch_at=actived_from)
    res = []

    for e in even : # iterate groups in per task.

        # check the name given by sheet user exist or not in OfferGroupCursor.groups, which means the snapshot in that moment.
        group_find = cursor.group_display(text_filter=e['group_name'], return_object=True) 

        if e['group_name'] == 'ba$e' : # setup default value if the input is `ba$e` in sheet.
            cursor.setup_default_value(rate=e['rate'], percent=e['percent'])
            res.append({'index' : e['index'], 'status': True, 'note': None})

            if e['force_resync_at'] : # setup `force_resync_at` value in sub_field
                cursor.setup_sub_fields(e['force_resync_at'])
            continue

        elif len(group_find) != 1 :  # if the number of group found with given name is not 1. 

            n = len(group_find) 

            if n == 0 : # if no group was found with given name.

                # seach name on HasOffers
                r = api_req(api_url, [
                    config_params,
                    {'Target':'CashflowGroup'},
                    {'Method':'findCashflowGroups'},
                    {'filters[name]' : e['group_name']}
                ])
                if r.json()['response']['data']['records'] : # append to OfferGroupCursor if group has been found externally.
                    cursor.group_append(r.json()['response']['data']['records'][0]['id'], rate=e['rate'], percent=e['percent'])
                    res.append({'index' : e['index'], 'status': True, 'note': None})
                    print('[Note] Append %s(%s) Externally.'%(e['group_name'], r.json()['response']['data']['records'][0]['id']))
                    continue
                else : # throw error if nothing was found
                    res.append({'index' : e['index'], 'status' : False, 'note' : '[Error] Bad Name With %s Group(s) Has Been Found Externally'%n})
                    break
     
            else : # throw error if there are more than one group matched with given name.
                res.append({'index' : e['index'], 'status' : False, 'note' : '[Error] Bad Name With %s Group(s) Has Been Found'%n})
                break

        if e['force_resync_at'] : # setup `force_resync_at` in sub column
            cursor.setup_sub_fields(e['force_resync_at'], cashflow_group_id = group_find[0].cashflow_group_id)
        
        group_find[0].setup_value(rate = e['rate'], percent = e['percent']) # setup group value 
        res.append({'index' : e['index'], 'status': True, 'note': None}) # result that need to return to sheet.

    cursor.setup_period(actived_from=e['actived_from'], actived_to=e['actived_to']) # setup period for snapshot in database.

    if False not in [x['status'] for x in res] : cursor.push_cursor() # push to database if there's no error.
    response += res       
    return response 