from dateutil.parser import parse
from typing import Union, List, Any
import datetime
import os
import ssl
import json
import asyncio
import aiohttp
import requests
import pymysql
import urllib.parse


from Group import Group, RulesTemplate
from util.database import sql_singleton


database = sql_singleton() # connect mysql server

with open(os.path.dirname(os.path.abspath(__file__)) + '/util/config.json') as f : # setup HasOffers creds
    creds = json.loads(f.read())['HasOffers']
    api_url = creds['api_url']
    config_params = {
        'NetworkToken' : creds['api_token']
    }

class OfferGroupsCursor :
    def __init__(self, offer_id :str, branch_at :str = None  , base_only :bool = False) :
        """
        Create an offer snapshot from any specified datetime.

        Arguments : 
            * offer_id (str): 
                offer_id on HasOffers.

            * branch_at (str | None): 
                None -> use current configuration on HasOffers.
                str -> find the snapshot with max(created_at) in database.
                base_only -> return the snapshot with its type as `base`.
        """

        self.offer_id = offer_id
        self.actived_from = None
        self.actived_to = None
        self.created_at = None
        self.__is_base = False if not base_only else True
        print('-'*10)
        print('''[Note] Start Fetching Group Data Of Offer %s'''%self.offer_id)
        start = datetime.datetime.now()
        self.groups = self.__get_id_list(branch_at=branch_at, base_only=base_only)
        print('''[Note] Finish Fethcing Group Data With %s Seconds'''%(datetime.datetime.now()-start))
        self.should_update = False
        if branch_at :
            self.__resync__(branch_at)

    def __get_id_list(self, branch_at :str = None, base_only :bool = False) :

        if branch_at : #fetch from database.
            cursor = database.cursor(pymysql.cursors.DictCursor)
            cursor.execute('''SELECT * FROM GroupSchedule WHERE 
            offer_id = '%s' AND
            created_at = (SELECT MAX(created_at) FROM 
            GroupSchedule WHERE  
            offer_id = '%s' AND 
            actived_from <= '%s' AND 
            actived_to > '%s' %s);'''%(self.offer_id, self.offer_id, branch_at, branch_at, "AND group_type = 'base'" if base_only else ''))
            raw_group = cursor.fetchall()
            if len(raw_group) == 0 : 
                return self.__get_id_list() # use current setting when there's no record in db.
            self.branch_at = branch_at
            self.created_at = raw_group[0]['created_at']
            default_payout = json.loads(raw_group[0]['default_value'])
            raw_group = [ x for x in raw_group if x['cashflow_group_id']] # exclude base group.

        else : #fetch from current online setting.
            r = api_req(api_url, [
                {'Target':'Offer'},
                {'Method':'findById'},
                {'id':self.offer_id},
                {'fields[]':'percent_payout'},
                {'fields[]':'max_payout'},
                {'fields[]' : 'max_percent_payout'},
                {'fields[]' : 'default_payout'},
                {'contain[]':'PayoutGroup'},
                config_params 
            ])
            if not r.json()['response']['errors'] :
                default_payout, raw_group = r.json()['response']['data']['Offer'], r.json()['response']['data']['PayoutGroup']
                raw_group = [] if raw_group is None else raw_group.values()
            else :
                raise ValueError(r.json()['response']['errors'])

            group_async_fetch_urls = [api_url + '?' + '&'.join(str(k) + '=' + str(v) for k, v in {
                'NetworkToken' : creds['api_token'],
                'Target':'CashflowGroup',
                'Method':'findCashflowGroupById', 
                'cashflow_group_id': cid
                }.items()) for cid in {x['cashflow_group_id'] for x in raw_group}
            ]
            result = AysncHandler(group_async_fetch_urls).output
            for g in result : 
                if not g['response']['errors'] :
                    cashflow_group_id = g['request']['cashflow_group_id']
                    for group in raw_group :
                        if cashflow_group_id == group['cashflow_group_id'] : 
                            group['rules'] = g['response']['data']['rules']
                            group['description'] = g['response']['data']['description']
                            group['affiliates'] = [ a['id'] for a in g['response']['data']['affiliates']]
                            group['name'] = g['response']['data']['name']
                            break
                else :
                    raise ValueError(r.json()['response']['errors'])
        
        self.groups = {}
        for group in raw_group :         
            self.groups[group['cashflow_group_id']] = Group(
                self.offer_id,
                group['cashflow_group_id'],
                group['percent'],
                group['rate'],
                name=group['name'], 
                description=group['description'], 
                affiliates=json.loads(group['affiliates']) if branch_at else group['affiliates'], 
                rules=json.loads(group['rules']) if branch_at else group['rules'],
                db_id=group['id'] if branch_at else None   
            )
           
        q = api_req(api_url, [
            {'Target':'Offer'},
            {'Method':'findById'},
            {'id':self.offer_id},
            {'fields[]':'payout_type'},
            config_params
        ])
        if not q.json()['response']['errors'] :
                self.ptype = q.json()['response']['data']['Offer']['payout_type']
        else :
            raise ValueError(r.json()['response']['errors'])
        
        self.setup_default_value(rate=default_payout['max_payout'], percent=default_payout['percent_payout'])
        if branch_at and len(raw_group) > 0 : 
            for rg in raw_group :
                self.groups[rg['cashflow_group_id']].actived_from = rg['actived_from']
                self.groups[rg['cashflow_group_id']].actived_to = rg['actived_to']

        return self.groups

    def __resync__(self, branch_at :str) :
        """
        Iteratively refresh groups, to adjust inherited group (follow = 1) in snapshot into correct value (follow = 0).
        This feature is to solve the confliction caused by several snapshot saved by different user, since the former created snapshot might get accidentally overrided.
        When method `setup_value` of a Group object get called, Group object will be assigned with follow = 0, we consider this execution is with intention.
        Once a snapshot is selected by job by its `created_at`, groups with follow = 1 in that snapshot will down search the same group with max(created_at) and follow = 0.
        """
        bs = []
        to_be_remove = []
        for cashflow_group_id, group in self.groups.items() :
            b = group.__refresh__(branch_at)
            if b >= 0 :
                bs.append(True)
                if b :
                    to_be_remove.append(str(b))
            else :
                bs.append(False)
        self.should_update = any(bs)
        for rm in to_be_remove : 
            self.group_remove(rm)


    def group_display(self, text_filter :str = None, value_filter :float = None, return_object :bool = False) -> Union[Group, str]:
        """
        Search groups inside the OfferGroupsCursor with given condition.

        Arguments :
            * text_filter (str):
                Use text string to search group name.
            * value_filter (float):
                Use float number to search matched CPS or CPA of group.
            * return_object (bool) :
                A Group object will be returned if set to true, otherwise return string of group description.

        Return :
            * Group Object (if return_object==True)
            * str (if return_object==False)
        """
        eligible_groups = []
        for group_id, group in self.groups.items():
            flag = False
            if text_filter :
                flag = text_filter.lower() in group.name.lower()
            if value_filter :
                percent = float(group.percent) if group.percent else None
                rate = float(group.rate) if group.rate else None
                flag = True if float(value_filter) in (percent, rate) else False
            if flag and group_id : eligible_groups.append(group_id)   
        return [self.groups[x].__dict__ for x in eligible_groups] if not return_object else [ self.groups[x] for x in eligible_groups]
    
    @classmethod
    def group_import(self, text_filter: str) -> Union[bool, Group]:
        """
        Search groups by its name externally on HasOffers.

        Arguments :
            * text_filter (str):
                Group name for searching on HasOffers

        Return :
            * Group Object (return the first group in API response)
            * None (if nothing found)
        """
        r = api_req(api_url, [
            config_params,
            {'Target':'CashflowGroup'},
            {'Method':'findCashflowGroups'},
            {'filters[name][LIKE]' : text_filter}
        ])
        if r.json()['response']['data']['records'] :
            cashflow_group_id = r.json()['response']['data']['records'][0]['id']
            return Group(None, cashflow_group_id, None, None, sync_current=True)
        else :
            return None

    def group_create(self, name: str , rules: List[RulesTemplate], percent: float = None, rate: float = None, affiliates: List[str] = [], desc : str = None) -> Group: 
        """
        Create group with given context and attach to OfferGroupCursor.

        Arguments :
            * name (str):
                Name of group, need to be unique on HasOffers.
            * rules (List[RulesTemplate]) :
                List of RulesTemplate.
            * percent (float) : 
                CPS for this group to attach to this offer.
            * rate (float) :
                CPA for this group to attach to this offer.
            * affiliates (List[str]) : 
                Whitelist affiliate_id on HasOffers, set empty to allow every affiliate.
            * desc (str) : 
                Description of this group.

        Return :
            * Group Object
        """
        if not all(isinstance(r, RulesTemplate) for r in rules) :
            TypeError('''[Error] Invalid Type Of Rules Instance.''')
        if not percent and not rate : raise ValueError('''[Error] Require Value Of Percent Or Rate''')
        r = api_req(api_url, [
            config_params, 
            {'Target':'CashflowGroup'},
            {'Method':'createCashflowGroup'},
            {'name':name + str(datetime.datetime.now().strftime('%Y-%m-%d'))},
            {'description':desc},
            {'affiliate_ids_json':'[%s]'%urllib.parse.quote(','.join(list(map(lambda x : json.dumps(x), affiliates))))},
            {'rules_json':'[%s]'%urllib.parse.quote(','.join(list(map(lambda x : json.dumps(x), Group.rules_serialize(*rules)))))}
        ]
        )
        if not r.json()['response']['errors'] :
            created = Group(self.offer_id, r.json()['response']['data'], percent, rate, sync_current=True)
            self.group_append(created.cashflow_group_id, percent=percent, rate=rate)
            return created
        else :
            raise ValueError(r.json()['response']['data'])

    def group_append(self, cashflow_group_id: str, percent: float = None, rate: float = None) -> None: 
        """
        Append group by its `cashflow_group_id` to OfferGroupCursor object.

        Arguments :
            * cashflow_group_id (str):
                Id of this group on HasOffers.
            * percent (float) : 
                CPS for this group to attach to this offer.
            * rate (float) :
                CPA for this group to attach to this offer.

        Return :
            * None
        """
        if not percent and not rate : raise ValueError('''[Error] Require Value Of Percent Or Rate''')
        if (self.ptype == 'cpa_both') and (percent is None or rate is None) :
            raise ValueError('[Error] Wrong Value Setup For %s Type Offer'%self.ptype)
        self.groups[str(cashflow_group_id)] = Group(self.offer_id, cashflow_group_id, percent, rate, follow=0, sync_current=True)

    def group_remove(self, cashflow_group_id: str) -> None: 
        """
        Remove group by its `cashflow_group_id` from OfferGroupCursor object.

        Arguments :
            * cashflow_group_id (str):
                Id of this group on HasOffers.
    
        Return :
            * None
        """
        self.groups.pop(cashflow_group_id)   
        
    def setup_sub_fields(self, sub_value: Any, index: int = 1, cashflow_group_id: str = None) -> None: 
        """
        Setup sub field for each group, each index is indicate to the respective column in database.

        Arguments :
            * sub_value (Any):
                Value to be saved in sub field.
            * index (int):
                The index of sub field.
            * cashflow_group_id (str) : 
                if None, will apply to every groups in OfferGroupCursor
                if id was specified, will set that group only.

        Return :
            * None
        """
        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''DESC GroupSchedule''')
        subs = [x['Field'] for x in cursor.fetchall() if "sub_" in x["Field"]]
        if 'sub_' + str(index) in subs :
            setattr(self, 'sub_' + str(index), sub_value)
            if not cashflow_group_id :
                for g in self.groups : setattr(self.groups[g], 'sub_' + str(index), sub_value)
            else :
                for g in self.groups : 
                    try :
                        setattr(self.groups[g], 'sub_' + str(index), self.groups[g].__getattribute__('sub_' + str(index)))
                    except :
                        setattr(self.groups[g], 'sub_' + str(index), None)
                setattr(self.groups[cashflow_group_id], 'sub_' + str(index), sub_value)
        else :
            raise IndexError('[Error] Invalid Sub Index For Passing Value')

    def setup_default_value(self, rate: float = 0, percent: float = 0) -> str :
        """
        Setup default CPS and CPA for offer.

        Arguments :
            * rate (float) : 
                default CPA for offer.
            * percent (float) :
                default CPS for offer

        Return :
            * JSON string of its current default configuration.
        """
        rate = rate if rate else 0
        percent = percent if percent else 0
        self.default_value = json.dumps({
            'max_payout' : rate,
            'default_payout' : rate,
            'max_percent_payout' : percent,
            'percent_payout' : percent
        })
        return self.default_value
        
    def setup_period(self, actived_from: str = None, actived_to: str = None, is_base: bool = False) -> None :
        """
        Setup the start time and end time for the snapshot that going to be deployed.

        Arguments :
            * actived_from (datetime str) : 
                start time of this snapshot.
            * actived_to :
                end time of this snapshot.
            * is_base : 
                set the snapshot as baseline of this offer. (every group in snapshot that get set as baseline will all become follow == 0)
        Return :
            * None
        """
        self.__is_base = False
        if is_base : actived_from, actived_to, self.__is_base = datetime.datetime.min.strftime('%Y-%m-%d %H:%M:%S'), datetime.datetime.max.strftime('%Y-%m-%d %H:%M:%S'), True
        dt_str = lambda x : parse(x) if isinstance(x, str) else x
        self.actived_from, self.actived_to = dt_str(actived_from), dt_str(actived_to)
        for cashflow_group_id, group in self.groups.items() :
            if dt_str(actived_from) > dt_str(actived_to) : raise ValueError('''[Error] Could Not Deactive Before Active.''')
            group.actived_from = dt_str(actived_from)
            group.actived_to = dt_str(actived_to)
            if is_base :
                group.follow = 0

    def push_cursor(self, stacking: bool = False) -> None:
        """
        Push current snapshot to database.

        Arguments :
            * stacking : If current snapshot was created by `branch_at` which means there's `created_at` attribute for this snapshot.
            After some changes are made on this snapshot and is ready for push, `stacking == True` could make the `created_at` of current push only 1 second after the `created_at` of branch.

        Return :
            * None
        """
        if not self.actived_from or not self.actived_to : 
            raise ValueError('''[Error] Period Need To Be Setup Before Push Cursor''')

        if len(self.groups) == 0 : # if there's no group in OfferGroupCursor, will create a base type group to save in database.
    
            self.groups = {'base' : Group(self.offer_id, None, None, None, follow=0)}

        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''DESC GroupSchedule''')
        cols = [x['Field'] for x in cursor.fetchall()]
        timestamp = datetime.datetime.now()
        if self.created_at and stacking :
            timestamp = self.created_at + datetime.timedelta(seconds=1)
            print('stacking', timestamp)
        columns = ""
        values = list()
        for cashflow_group_id, group in self.groups.items() : 
            group.status = 'pending'
            group.group_type = 'tmp' if not self.__is_base else 'base'
            group.created_at = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            group.actived_to = self.actived_to
            group.actived_from = self.actived_from
            group.default_value = self.default_value
            for sub_field in [x for x in self.__dict__ if 'sub' in x] :
                try : 
                    group.__getattribute__(sub_field)
                except :
                    setattr(group, sub_field, self.__getattribute__(sub_field))
            columns = "(%s)"%','.join([x for x in group.__dict__ if x in cols])
            values.append("('%s')"%("','".join(str(x).replace("'", '"') if not any([
                isinstance(x, dict),
                isinstance(x, list),
                isinstance(x, tuple),
                isinstance(x, set),
            ]) else json.dumps(x, ensure_ascii=False).replace("'", "''") for x in [group.__getattribute__(x) for x in group.__dict__ if x in cols]))) 
        insert_string = "INSERT INTO GroupSchedule %s VALUES %s"%(columns, ','.join(values))
        insert_string = insert_string.replace("'None'", "null")
        try : 
            cursor.execute(insert_string)
        except Exception as e :
            raise ValueError(e, insert_string)

    def __attach_enabler(self, b: int) -> None:
        """
        [1st step of deployment]
        Enable or disable payout group in offer by checking the group count of OfferGroupCursor.

        Arguments :
            * b (tinyint) : b == 1 will enable payout group, b == 0 will disable payout group.

        Return :
            * None
        """
        for i in ['data[use_payout_groups]', 'data[use_revenue_groups]'] :
            r = api_req(api_url, [
                config_params,
                {'Target':'Offer'},
                {'Method':'update'},
                {'id':self.offer_id}, 
                {i:b},
                {'return_object':0}
            ])
            if not r.json()['response']['errors'] :
                print('''[Note] Enabler Attached''')
            else :
                raise ValueError(r.json()['response']['errors'])

    def __attach_offer(self) -> None:
        """
        [2nd step of deployment]
        Attach payout groups and their value according to `.groups`.

        Arguments :
            * None
        Return :
            * None
        """
        for i, j in {'replaceOfferPayoutGroupsForOffer':'offer_payout_groups_json', 'replaceOfferRevenueGroupsForOffer':'offer_revenue_groups_json'}.items() : #attach groups to offer.
            r = api_req(api_url, [
                {'Target':'CashflowGroup'},
                {'Method':i},
                {'offer_id':self.offer_id},
                { j :
                '[%s]'%urllib.parse.quote(','.
                join(['{"cashflow_group_id":"%s" %s %s}'%(
                    x.cashflow_group_id, 
                    '' if not x.percent and self.ptype not in ('cpa_percentage', 'cpa_both') else ',"percent":"%s"'%(x.percent if x.percent else 0),
                    '' if not x.rate and self.ptype not in ('cpa_flat', 'cpa_both') else ',"rate":"%s"'%(x.rate if x.rate else 0)) 
                    for i, x in self.groups.items() if x.cashflow_group_id]))},
                config_params
            ])
            if not r.json()['response']['errors'] :
                    print('[Note] Offer Attached Group')
            else :
                raise ValueError(r.json()['response']['errors'])

    def __attach_value(self) -> None:
        """
        [3rd step of deployment]
        Setup the default CPS/ CPA of the offer according to `.default_value`.

        Arguments :
            * None
        Return :
            * None
        """
        for j in ['percent_payout', 'max_payout', 'max_percent_payout', 'default_payout'] : #setup value for group
            if (self.ptype == 'cpa_percentage' and j in ('max_payout', 'default_payout')) or (self.ptype == 'cpa_flat' and j in ('percent_payout','max_percent_payout')) :
                continue
            r = api_req(api_url, [
                {'Target':'Offer'},
                {'Method':'updateField'},
                {'id':self.offer_id},
                {'field':j},
                {'value':json.loads(self.default_value)[j]},
                config_params
              ]
            )
            if not r.json()['response']['errors'] :
                print('[Note] Group Value Attached')
            else :
                raise ValueError(r.json()['response']['errors'])

    def __attach_rules(self) -> None:
        """
        [4th step of deployment]
        Setup the rule of each payout group.

        Arguments :
            * None
        Return :
            * None
        """
        for k, v in self.groups.items():
            for rule in v.rules :
                rule.pop('id', None)
                rule.pop('cashflow_group_id', None)
            r = api_req(api_url, [
                config_params,
                {'Target':'CashflowGroup'},
                {'Method':'updateCashflowGroup'},
                {'id': v.cashflow_group_id},
                {'name': v.name},
                {'description': v.description},
                {'affiliate_ids_json':'[%s]'%urllib.parse.quote(','.join(list(map(lambda x : json.dumps(x), v.affiliates))))},
                {'rules_json':'[%s]'%urllib.parse.quote(','.join(list(map(lambda x : json.dumps(x), v.rules))))}]
            )
            if not r.json()['response']['errors'] :
                print('[Note] Rules Attached To Group %s'%k)
            else :
                raise ValueError(r.json()['response']['errors'])

    def __attach_schedule(self, utc) :
        """
        [DEPRECIATE]
        Arguments :
            * utc : timezone
        Return :
            * None
        """
        fallback = OfferGroupsCursor(self.offer_id, branch_at=self.actived_to).default_value
        for t in [self.actived_from, self.actived_to] :
            for g in ['percent_payout', 'max_payout', 'max_percent_payout', 'default_payout'] :
                if (self.ptype == 'cpa_percentage' and g in ('max_payout', 'default_payout')) or (self.ptype == 'cpa_flat' and g in ('percent_payout','max_percent_payout')) :
                    continue
                r = api_req(api_url, [
                    config_params,
                    {'Target' : 'ScheduledOfferChange'},
                    {'Method' : 'create'},
                    {'data[status]' : 'active'},
                    {'data[update_time_utc]' : t - datetime.timedelta(hours=utc)},
                    {'data[update_value]' : json.loads(self.default_value)[g] if t == self.actived_from else json.loads(fallback)[g]},
                    {'data[update_field]' : g},
                    {'data[offer_id]' : self.offer_id},
                    {'data[model]' : 'Offer'},
                ])
                if not r.json()['response']['errors'] :
                        print('[Note] %s %s Attached Schedule'%(g, t))
                else :
                    raise ValueError(r.json()['response']['errors'])

    def deploy_cursor(self) : 
        """
        Make change to offer when it's going to be actived.

        Arguments :
            * None
        Return :
            * None
        """
        if not self.created_at :
            raise ValueError('[Error] Push Before Deploy Cursor.')
        print('[Note] Start Processing Groups - %s'%list(self.groups.keys()))
        self.__attach_enabler(1 if len(self.groups) > 0 else 0)
        self.__attach_offer()
        self.__attach_rules()
        self.__attach_value()
        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''UPDATE GroupSchedule SET status = 'actived' WHERE created_at = '%s' '''%self.created_at)


class AysncHandler :
    
    def __init__(self, urls) :
        self.urls = urls
        self.loop = asyncio.get_event_loop()
        self.output = self.loop.run_until_complete(self.fetch_all(self.urls, self.loop))

    async def fetch(self, session, url) : 
        async with session.get(url, ssl=ssl.SSLContext()) as response :
            return await response.json()
    
    async def fetch_all(self, urls, loop) : 
        async with aiohttp.ClientSession(loop=loop) as session : 
            results = await asyncio.gather(*[self.fetch(session, url) for url in urls], return_exceptions=True)
            return results

def api_req(url, key_value_pair) :
    url += '?'
    params_list = list()
    for kv in key_value_pair :
        params_list += [str(k) + '=' + str(v) for k, v in kv.items()]
    url += '&'.join(params_list)
    r = requests.get(url)
    return r 
    

def job(even : Any, context : Any, local_utc: int = 8) -> None:
    """
    This function will need to be called repeatedly with certain interval by job (e.g cronjob, cloudwatch event).

    Function will do the following jobs : 
    1. if `current datetime >= snapshot actived_from && status == 'pending'`, snapshot will be load as OfferGroupCursor and deploy to HasOffers.
    2. if `current datetime >= snapshot actived_to && status == 'active'`, this snapshot status will be updated into `close`
    3. if any group `follow == 1 && status == 'active'`, will down search another group with `follow == 0` with max(created_at) in snapshot with lower priority to correct the value.
    
    Arguments : 
        * even : lambda default argument.
        * context : lambda default argument.
        * local_utc (int) : timezone for current time.

    Returns : 
        * None
    """
    t = (datetime.datetime.now() + datetime.timedelta(hours=local_utc))
    t = datetime.datetime.strptime('%s-%s-%s %s:00:00'%(t.year, t.month, t.day, t.hour), '%Y-%m-%d %H:%M:%S')
    cursor = database.cursor(pymysql.cursors.DictCursor)
    cursor.execute('''SELECT offer_id, MAX(created_at) AS created_at FROM GroupSchedule WHERE actived_from <= '%s' AND actived_to >= '%s' AND status != 'close' GROUP BY offer_id '''%(t,t))
    groups = cursor.fetchall()
    for group in groups :
        cursor.execute("""SELECT * FROM GroupSchedule WHERE offer_id = '%s' AND created_at = '%s'"""%(group['offer_id'], group['created_at'])) #select max created_at from each offer in this interval
        patches = cursor.fetchall()
        patch = patches[0]
        if patch :
            if patch['status'] == 'pending' :
                print('''[Note] Pending Task Found''')
                c = OfferGroupsCursor(patch['offer_id'], branch_at=t)
                try :
                    c.deploy_cursor()
                except Exception as e :
                    print('''[Error] Error Occur When Deploy Offer %s : %s'''%(patch['offer_id'], e))
                    continue
                cursor.execute("""UPDATE GroupSchedule SET status = 'pending' WHERE created_at < '%s' and offer_id = '%s' """%(patch['created_at'], patch['offer_id']))

            if patch['status'] == 'actived' and patch['group_type'] != 'base' :
                c = OfferGroupsCursor(patch['offer_id'], branch_at=t)
                if c.should_update :
                    print('''[Note] Task Required Update Found.''')
                    c = OfferGroupsCursor(patch['offer_id'], branch_at=t)
                    try :
                        c.deploy_cursor()
                    except Exception as e :
                        print('''[Error] Error Occur When Deploy Offer %s : %s'''%(patch['offer_id'], e))
                        continue

    cursor.execute("""UPDATE GroupSchedule SET status = 'close' WHERE actived_to <= '%s' """%t)

if __name__ == '__main__' : 
    job(1, 1)

