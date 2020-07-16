import collections
import datetime
import json
from dateutil.parser import parse
import requests
import pymysql
import urllib.parse
import re

database = pymysql.connect(host='13.114.105.152', port=3306, user='Ops', password='iloveShopBack!4', database='ShopBack', charset='utf8', autocommit='true')
api_url = 'https://shopback.api.hasoffers.com/Apiv3/json'
config_params = {
    'NetworkToken' : 'NETJTqTrjj38XObdBhygFOQ2ULa1V2'
}
'''
v 1.02 **
Bug : None
Feature : 
- Get group attach externally,
- Dynamically handle group consistency in different time frame.
'''
class OfferGroupsCursor :
    def __init__(self, offer_id, aff_id=1059, branch_at=None, base_only=False) :
        self.offer_id = offer_id
        self.aff_id = aff_id
        self.actived_from = None
        self.actived_to = None
        self.created_at = None
        self.__is_base = False if not base_only else True
        self.groups = self.__get_id_list(branch_at=branch_at, base_only=base_only)    

    def __get_id_list(self, branch_at=None, base_only=False) :
        if branch_at :
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
        else :
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
        self.ptype = api_req(api_url, [{'Target':'Offer'},{'Method':'findById'},{'id':self.offer_id},{'fields[]':'payout_type'},config_params]).json()['response']['data']['Offer']['payout_type']
        self.groups = {x['cashflow_group_id'] : Group(
            self.offer_id, 
            x['cashflow_group_id'], 
            x['percent'], 
            x['rate'], 
            rules=json.loads(x['rules']) if branch_at else None, 
            db_id=x['id'] if branch_at else None) for x in raw_group}
        self.setup_default_value(rate=default_payout['max_payout'], percent=default_payout['percent_payout'])
        self.setup_period
        if branch_at and len(raw_group) > 0 : 
            for rg in raw_group :
                self.groups[rg['cashflow_group_id']].actived_from = rg['actived_from']
                self.groups[rg['cashflow_group_id']].actived_to = rg['actived_to']
        return self.groups

    def group_display(self, text_filter=None, value_filter=None, return_object=False) :
        eligible_groups = []
        for group_id, group in self.groups.items():
            flag = False
            if text_filter :
                flag = text_filter.lower() in group.name.lower() + ' ' + group.description.lower()
            if value_filter :
                percent = float(group.percent) if group.percent else None
                rate = float(group.rate) if group.rate else None
                flag = True if float(value_filter) in (percent, rate) else False
            if flag and group_id : eligible_groups.append(group_id)   
        return [self.groups[x].__dict__ for x in eligible_groups] if not return_object else [ self.groups[x] for x in eligible_groups]
    
    @classmethod
    def group_import(self, text_filter) :
        r = api_req(api_url, [
            config_params,
            {'Target':'CashflowGroup'},
            {'Method':'findCashflowGroups'},
            {'filters[name][LIKE]' : text_filter}
        ])
        if r.json()['response']['data']['records'] :
            cashflow_group_id = r.json()['response']['data']['records'][0]['id']
            return Group(None, cashflow_group_id, None, None)
        else :
            return None

    def group_create(self, name, rules, percent=None, rate=None, affilaites=[], desc=None, overflow_bulk_create=False) : 
        if not all(isinstance(r, RulesTemplate) for r in rules) :
            TypeError('''[Error] Invalid Type Of Rules Instance.''')
        if not percent and not rate : raise ValueError('''[Error] Require Value Of Percent Or Rate''')
        r = api_req(api_url, [
            config_params, 
            {'Target':'CashflowGroup'},
            {'Method':'createCashflowGroup'},
            {'name':name + str(datetime.datetime.now().strftime('%Y-%m-%d'))},
            {'description':desc},
            {'affiliate_ids_json':'[%s]'%urllib.parse.quote(','.join(list(map(lambda x : json.dumps(x), affilaites))))},
            {'rules_json':'[%s]'%urllib.parse.quote(','.join(list(map(lambda x : json.dumps(x), Group.rules_serialize(*rules)))))}
        ]
        )
        if not r.json()['response']['errors'] :
            created = Group(self.offer_id, r.json()['response']['data'], percent, rate)
            self.group_append(created.cashflow_group_id, percent=percent, rate=rate)
            return created
        else :
            raise ValueError(r.json()['response']['data'])

    def group_append(self, cashflow_group_id, percent=None, rate=None) : 
        if not percent and not rate : raise ValueError('''[Error] Require Value Of Percent Or Rate''')
        if (self.ptype == 'cpa_both') and (percent is None or rate is None) :
            raise ValueError('[Error] Wrong Value Setup For %s Type Offer'%self.ptype)
        self.groups[str(cashflow_group_id)] = Group(self.offer_id, cashflow_group_id, percent, rate, follow=0)

    def group_remove(self, cashflow_group_id) : 
        self.groups.pop(cashflow_group_id)   

    def group_inject(self, field, operator, value, percent=None, rate=None, negate=0, inherit_rules=True) :
        if len(self.groups) == 0 :
            raise ValueError('[Error] No Group Was Included In This Offer')
        if not percent and not rate :
            raise ValueError('[Error] Percent Or Rate Need To Be Specified For Appending')
        for group in self.groups :
            if group.__require_fields__(field, operator) and (float(percent) == float(group.percent) or float(rate) == float(group.rate)) and group.rules :
                group.define_configure(rules=RulesTemplate(field, operator, value, negate=negate), inherit_rules=True)
                break
        
    def setup_sub_fields(self, sub_value, index=1) : 
        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''DESC GroupSchedule''')
        subs = [x['Field'] for x in cursor.fetchall() if "sub_" in x["Field"]]
        if 'sub_' + str(index) in subs :
            for g in self.groups : setattr(self.groups[g], 'sub_' + str(index), sub_value)
        else :
            raise IndexError('[Error] Invalid Sub Index For Passing Value')

    def setup_default_value(self, rate=0, percent=0) :
        rate = rate if rate else 0
        percent = percent if percent else 0
        self.default_value = json.dumps({
            'max_payout' : rate,
            'default_payout' : rate,
            'max_percent_payout' : percent,
            'percent_payout' : percent
        })
        return self.default_value
        
    def setup_period(self, actived_from=None, actived_to=None, is_base=False) :
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

    def push_cursor(self, stacking=False) :
        if not self.actived_from or not self.actived_to : 
            raise ValueError('''[Error] Period Need To Be Setup Before Push Cursor''')
        if len(self.groups) == 0 :
            # if not self.__is_base :
            #     self.__attach_enabler(0)
            #     return self.__attach_schedule(utc)
            # if self.__is_base : 
            self.groups = {'base' : Group(self.offer_id, None, None, None, is_base=True)}
        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''DESC GroupSchedule''')
        cols = [x['Field'] for x in cursor.fetchall()]
        timestamp = datetime.datetime.now()
        if self.created_at and stacking :
            timestamp += datetime.timedelta(seconds=1)
        columns = ""
        values = list()
        for cashflow_group_id, group in self.groups.items() : 
            group.status = 'pending'
            group.group_type = 'tmp' if not self.__is_base else 'base'
            group.created_at = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            group.actived_to = self.actived_to
            group.actived_from = self.actived_from
            group.default_value = self.default_value
            
            columns = "(%s)"%','.join([x for x in group.__dict__ if x in cols])
            values.append("('%s')"%("','".join(str(x).replace("'", '"') if not any([
                isinstance(x, dict),
                isinstance(x, list),
                isinstance(x, tuple),
                isinstance(x, set),
            ]) else json.dumps(x).replace("'", "''") for x in [group.__getattribute__(x) for x in group.__dict__ if x in cols])))
        insert_string = "INSERT INTO GroupSchedule %s VALUES %s"%(columns, ','.join(values))
        insert_string = insert_string.replace("'None'", "null")
        try : 
            cursor.execute(insert_string)
        except Exception as e :
            raise ValueError(e, insert_string)

    def __attach_enabler(self, b) :
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

    def __attach_offer(self) :
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

    def __attach_value(self) :
        for j in ['percent_payout', 'max_payout', 'max_percent_payout', 'default_payout'] : #setup value for group
            if (self.ptype == 'cpa_percentage' and j in ('max_payout', 'default_payout')) or (self.ptype == 'cpa_flat' and j in ('percent_payout','max_percent_payout')) :
                continue
            r = api_req(api_url, [
                {'Target':'Offer'},
                {'Method':'updateField'},
                {'id':self.offer_id},
                {'field':j},
                {'value':json.loads(self.default_value )[j]},
                config_params
              ]
            )
            if not r.json()['response']['errors'] :
                print('[Note] Group Value Attached')
            else :
                raise ValueError(r.json()['response']['errors'])

    def __attach_rules(self) :
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
        if not self.created_at :
            raise ValueError('[Error] Push Before Deploy Cursor.')
        print('[Note] Start Processing Groups - %s'%list(self.groups.keys()))
        self.__attach_enabler(1 if len(self.groups) > 0 else 0)
        self.__attach_offer()
        self.__attach_rules()
        self.__attach_value()
        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''UPDATE GroupSchedule SET status = 'actived' WHERE created_at = '%s' '''%self.created_at)

class Group :
    '''
    group created by multi clauses and its "value", "attached direction"
    '''
    def __init__(self, offer_id, cashflow_group_id, percent, rate, follow=1, rules=None, is_base=False, db_id=None) :
        if not is_base :
            r = api_req(api_url, [
                config_params,
                {'Target':'CashflowGroup'},
                {'Method':'findCashflowGroupById'}, 
                {'cashflow_group_id':cashflow_group_id}
            ])
            if not r.json()['response']['errors'] :
                self.name = r.json()['response']['data']['name']
                self.description = r.json()['response']['data']['description']
                self.affiliates = [ a['id'] for a in r.json()['response']['data']['affiliates']]
                self.rules = r.json()['response']['data']['rules'] if not rules else rules
            else :
                raise ValueError(r.json()['response']['errors'])    
        self.cashflow_group_id = cashflow_group_id
        self.offer_id = offer_id
        self.percent = float(percent) if percent else None
        self.rate = float(rate) if rate else None
        self.db_id = db_id
        if rate :
            if ',' in str(rate) :
                self.rate = float(str(rate).replace('.', '').replace(',', '.'))
        self.follow = follow

    def __refresh__(self, t) :
        cursor = database.cursor(pymysql.cursors.DictCursor)
        result = None
        if self.follow == 1 :
            cursor.execute('''
            SELECT percent, rate FROM GroupSchedule 
            WHERE cashflow_group_id = %s
            AND status = 'pending'
            AND offer_id = %s
            AND actived_from <= '%s'
            AND actived_to >= '%s'
            AND follow = 0
            ORDER BY created_at DESC LIMIT 1;
            '''%(self.cashflow_group_id, self.offer_id, t, t))
            result = cursor.fetchall()
        
        if result :
            result = result[0]
            if float(sum(filter(None, list(result.values())))) != float(sum(filter(None, [self.percent, self.rate]))):
                cursor.execute('''UPDATE GroupSchedule SET percent = %s, rate = %s WHERE id = %s'''%(result['percent'] if result['percent'] else 0, result['rate'] if result['rate'] else 0, self.db_id))
                self.percent = result['percent']
                self.rate = result['rate']
                print('''[Note] Group %s of Offer %s Has Been Updated Into - Percent : %s, Rate : %s '''%(self.cashflow_group_id, self.offer_id, result['percent'], result['rate']))
                return True
        else :
            return False

    def __require_fields__(self, y, z) :
        return (y, z) in [ (x['field'], x['operator']) for x in self.rules]
    
    def setup_value(self, percent=None, rate=None) :
        self.percent = float(percent) if percent else None
        self.rate = float(rate) if rate else None
        self.follow = 0 if percent or rate else 1
        
    def define_configure(self, name=None, description=None, affiliates=None, rules=[], inherit_rules=True, limit=250) :
        added = []
        for i in rules : 
            if not isinstance(i, RulesTemplate) : 
                raise TypeError('''[Error] Invalid Type Of Rules Instance.''')
            else :
                added.append(i.__dict__)
        total = added + self.rules
        r = self.rules_serialize(*total) if inherit_rules else self.rules_serialize(*rules)
        if sum([len(x['value']) for x in r]) > limit : 
            raise ValueError('''[Error] Length Of Rules Has Already Exceeded The Limitation''')
        self.name = name if name else self.name
        self.description = description if description else self.description
        self.affiliates = affiliates if affiliates else self.affiliates
        self.rules = r

    @classmethod   
    def rules_serialize(self, *args) :
        serialized = collections.defaultdict(list)
        for rule in args :
            indicator = '%s-%s-%s'%(rule.field, rule.operator, rule.negate)
            serialized[indicator]+= rule.value
        rules = [ 
            {'field' : k.split('-')[0],
            'operator' : k.split('-')[1],
            'value' : list(set(v)),
            'negate' : k.split('-')[2]
            } for k, v in serialized.items()
        ]
        return rules

class RulesTemplate :
    field_const = [
        'advanced_targeting_rule',
        'aff_sub1',
        'aff_sub2',
        'aff_sub3',
        'aff_sub4',
        'aff_sub5',
        'adv_sub1',
        'adv_sub2',
        'adv_sub3',
        'adv_sub4',
        'adv_sub5',
        'country',
        'source']
    operator_const = [
        'IS',
        'STARTS_WITH',
        'CONTAINS',
        'ENDS_WITH'
    ]
    
    def __init__(self, field, operator, value, negate=0) :
        if field not in RulesTemplate.field_const : raise ValueError('''value of field should be one of %s.'''%RulesTemplate.field_const)
        if operator not in RulesTemplate.operator_const : raise ValueError('''value of operator should be one of %s.'''%RulesTemplate.operator_const)
        if not isinstance(value, list) : raise TypeError('''rules should be set in list type.''')
        self.field = field
        self.operator = operator
        self.value = value
        self.negate = negate

def api_req(url, key_value_pair) :
    url += '?'
    params_list = list()
    for kv in key_value_pair :
        params_list += [str(k) + '=' + str(v) for k, v in kv.items()]
    url += '&'.join(params_list)
    r = requests.get(url)
    return r 
    
def job(even, context, local_utc=8) :
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
            if patch['status'] == 'pending' : #activate group.
                print('[Note] %s Job Found For %s'%(str(len(groups)) + ', Fallback To Base' if patch['group_type'] == 'base' else '', patch['offer_id']))
                c = OfferGroupsCursor(patch['offer_id'], branch_at=t)
                try :
                    c.deploy_cursor()
                except Exception as e :
                    print('''[Error] Error Occur When Deploy Offer %s : %s'''%(patch['offer_id'], e))
                    continue
                cursor.execute("""UPDATE GroupSchedule SET status = 'pending' WHERE created_at < '%s' and offer_id = '%s' """%(patch['created_at'], patch['offer_id']))    
            elif patch['status'] == 'actived' and patch['group_type'] != 'base' and patch['cashflow_group_id']:
                c = OfferGroupsCursor(patch['offer_id'], branch_at=t)
                flag = [c.groups[x['cashflow_group_id']].__refresh__(t) for x in patches if x['follow'] == 1]
                if True in flag : 
                    c.deploy_cursor()

    cursor.execute("""UPDATE GroupSchedule SET status = 'close' WHERE actived_to <= '%s' """%t)

