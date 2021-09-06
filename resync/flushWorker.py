import json
import requests
import asyncio
import aiohttp
import ssl
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/..') # Can setup in lambda layer instead.
from GroupSchedule import api_url, config_params

def lambda_handler(even, config) :
    getData(even)
    return

def getData(payload) : 
    # pull conversion from HasOffers with (offer_id, cashflow_group_id, datetime) as condition parameter.
    params = [
        config_params,
        {'Target' : 'Report'},
        {'Method' : 'getConversions'},
        {'fields[]' : 'PayoutGroup.id'},
        {'fields[]' : 'Stat.payout'},
        {'fields[]' : 'Stat.sale_amount'},
        {'fields[]' : 'Stat.advertiser_info'},
        {'fields[]' : 'Stat.currency'},
        {'fields[]' : 'Stat.tune_event_id'},
        {'filters[Stat.datetime][conditional]' : 'BETWEEN'},
        {'filters[Stat.datetime][values][]' : payload['from']},
        {'filters[Stat.datetime][values][]' : payload['to']},
        {'filters[Stat.offer_id][conditional]' : 'EQUAL_TO'},
        {'filters[Stat.offer_id][values]' : payload['offer_id']},
        {'filters[PayoutGroup.id][conditional]' : 'EQUAL_TO'}
    ]
    # cashflow_group_id of groups that will need adjustment.
    for i, c in enumerate(payload['context']) :
        params.append({'filters[PayoutGroup.id][values][]' : c['cashflow_group_id']})
    # conversion without payout group.
    params.append({'filters[PayoutGroup.id][values][]' : '0'})
    response = requests.get(concatURL(api_url, params))
    output = []
    if not response.json()['response']['errors'] and response.json()['response']['data']['pageCount']:
        pages = response.json()['response']['data']['pageCount']
        urls = [concatURL(api_url, params + [{'page' : i}]) for i in range(1, pages + 1)]
        result = AsyncHandler(urls, 3).output # Sempaphore 3 here.
        for res in result : output += res['response']['data']['data']
        processData(payload, output)

def processData(payload, data) :
    update = []
    groupReference = {x['cashflow_group_id'] : {'percent' : x['percent'], 'rate' : x['rate']} for x in payload['context']}
    baseReference = {'percent' : json.loads(payload['default_value'])['max_percent_payout'], 'rate' : json.loads(payload['default_value'])['max_payout']}
    for d in data :
        payout = float(d['Stat']['payout@' + d['Stat']['currency']])
        sale_amount = float(d['Stat']['sale_amount@' + d['Stat']['currency']])
        cashflow_group_id = d['PayoutGroup']['id']
        to_be_updated_id = d['Stat']['tune_event_id']
        to_be_updated_payout = None

        # start evaludation, once below operation found the current value doesn't matcch to the correct reference from payload given by Master, conversion will be append into list for update.
        try :
            #transaction without payout group
            if cashflow_group_id == '0' : 
                if baseReference['percent'] and sale_amount : 
                    if round(payout/sale_amount, 3) != float(baseReference['percent'])/100 :
                        to_be_updated_payout = round(sale_amount * float(baseReference['percent'])/ 100, 3)
                else :
                    if payout != float(baseReference['rate']) :
                        to_be_updated_payout = float(baseReference['rate'])
            #transaction with payout group
            else :
                group_context = groupReference[d['PayoutGroup']['id']]
                if group_context['percent'] : 
                    if round(payout/sale_amount, 3) != float(group_context['percent'])/100 :
                        to_be_updated_payout = round(sale_amount * float(group_context['percent']) / 100, 3)
                else :
                    if payout != float(group_context['rate']) :
                        to_be_updated_payout = float(group_context['rate'])
            if to_be_updated_payout :
                print('PayoutGroup : %s, Payout : %s, Sale Amount : %s, OrderID : %s'%(d['PayoutGroup']['id'], payout, sale_amount, d['Stat']['advertiser_info']))
                print('current percent : %s, correct percent : %s, updated payout : %s'%(round(payout/sale_amount, 3), round(to_be_updated_payout/sale_amount,3), to_be_updated_payout))
                print('----------------------')
                update.append({'tune_event_id' : to_be_updated_id, 'payout' : to_be_updated_payout})
        except Exception as e:
            print(d, e)
            continue
    updateData(update)

def updateData(update) :
    # Start updating incorrect conversion.
    params = [
        config_params,
        {'Target' : 'Conversion'},
        {'Method' : 'updateField'},
        {'field' : 'payout'}
    ]
    urls = [concatURL(api_url, params + [{'tune_event_id' : x['tune_event_id']}, {'value' : x['payout']}]) for x in update]
    async_call = AsyncHandler(urls, 1).output # Semaphore 1 here.
    errorCount = [x['response']['errorMessage'] for x in async_call if x['response']['errorMessage']]
    print('Finish Flushing - Process Count : %s, Error Count : %s'%(len(urls), len(errorCount)))
    print(errorCount)

class AsyncHandler :
     
    def __init__(self, urls, sem) :
        self.sem = asyncio.Semaphore(sem)
        self.urls = urls
        self.loop = asyncio.get_event_loop()
        self.output = self.loop.run_until_complete(self.fetch_all(self.urls, self.loop))

    async def fetch(self, session, url) : 
        async with self.sem :
            async with session.get(url, ssl=ssl.SSLContext()) as response :
                return await response.json()
        
    async def fetch_all(self, urls, loop) : 
        async with aiohttp.ClientSession(loop=loop) as session : 
            results = await asyncio.gather(*[self.fetch(session, url) for url in urls], return_exceptions=True)
            return results

def concatURL(url, key_value_pair) :
    params_list = [str(list(x)[0]) + '=' + str(x[list(x)[0]]) for x in key_value_pair]
    return url + '?' + '&'.join(params_list)
