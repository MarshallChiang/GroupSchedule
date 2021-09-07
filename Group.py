from typing import Union, NamedTuple, List, Any
import collections
import pymysql
import os
import json
import requests
from util.database import sql_singleton


database = sql_singleton() # connect mysql server

with open(os.path.dirname(os.path.abspath(__file__)) + '/util/config.json') as f : # setup HasOffers creds
    creds = json.loads(f.read())['HasOffers']
    api_url = creds['api_url']
    config_params = {
        'NetworkToken' : creds['api_token']
    }

class RulesTemplate :
    """
    Template for creating rules, only `field` and `operator` defined below are allowed.
    
    Arguments : 
    * field (str) : field to be used for evaluation on HasOffers.
    * operator (str) : operator for evaluation.
    * value (List[str]) : the operand for evaluation 
    * negate (tinyint) : equal to `NOT`
    """
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


class Group :
    '''
    Group object created by OfferGroupCursor.
    
    Arguments :
    * offer_id (str): Id of the offer that this group is attaching to.
    * cashflow_group_id (str): Id of this payout group on HasOffers.
    * percent (float): CPS of this group set in offer.
    * rate (float): CPA of this group set in offer.
    * name (str): Name of this payout group.
    * description (str): Description of this payout group.
    * affiliates (List[str]): List of whitelist affiliate id on HasOffers
    * rules (List[RulesTemplate]): List of RulesTemplate object.
    * db_id (int): The id(pk) of this group in database. 
    * follow (tinyint): default to be 1 (means inherited from HasOffers/ database), if `setup_value` has been called or OfferGroupCursor has set the snapshot to baseline by `setup_period(is_base=True)`, follow will turn into 0, we consider this group should always be reflected when `job` is checking.
    * sync_current (bool) : sync with current configuration on HasOffers.
    '''
    def __init__(self, offer_id: str, cashflow_group_id: str, percent: float, rate: float, name: str = None, description: str = None, affiliates: List[str] = None, rules: List[RulesTemplate] = None, db_id: int = None, follow: int = 1, sync_current: bool = False) :
        
        self.offer_id = offer_id
        self.cashflow_group_id = cashflow_group_id
        self.percent = float(percent) if percent else None
        self.rate = float(rate) if rate else None
        self.name = name
        self.description = description
        self.affiliates = affiliates
        self.rules = rules
        self.db_id = db_id
        self.follow = follow
        if rate :
            if ',' in str(rate) :
                self.rate = float(str(rate).replace('.', '').replace(',', '.')) #Indonesia currency format bug fix.
        if sync_current :
            self.__sync_current__()

    def __refresh__(self, t: str) -> None:
        """
        Called by `job` when condition is met, down search the max(created_at) `follow == 0` group within same `cashflow_group_id`, `offer_id` and valid period.

        Arguments : 
        * t (datetime str) : usually provide by `job` , and the datetime should be current.

        Returns : 
        * None
        """
        cursor = database.cursor(pymysql.cursors.DictCursor)
        result = None
        if self.follow == 1 :
            cursor.execute('''
            SELECT percent, rate FROM GroupSchedule 
            WHERE cashflow_group_id = %s
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
    #                 cursor.execute('''UPDATE GroupSchedule SET percent = %s, rate = %s WHERE id = %s'''%(result['percent'] if result['percent'] else 0, result['rate'] if result['rate'] else 0, self.db_id))
                    self.percent = result['percent']
                    self.rate = result['rate']
                    print('''[Note] Group %s of Offer %s Has Been Updated Into - Percent : %s, Rate : %s '''%(self.cashflow_group_id, self.offer_id, result['percent'], result['rate']))
                    return 0
                else :
                    return -1
            else :
                print('''[Note] Group %s of Offer %s Will Be Dropped Since No Parent Is Found.'''%(self.cashflow_group_id, self.offer_id))
                return int(self.cashflow_group_id)
        else :
            return -1

    def __sync_current__(self) -> None :
        """
        Sync and applied the current configuration from HasOffers to this group.

        Arguments : 
        * None 

        Returns :
        * None
        """
        r = api_req(api_url, [
                {'contain[]':'PayoutGroup'},
                {'Target':'CashflowGroup'},
                {'Method':'findCashflowGroupById'}, 
                {'cashflow_group_id': self.cashflow_group_id},
                config_params 
            ])
        if not r.json()['response']['errors'] :
            d = r.json()
            self.name = d['response']['data']['name']
            self.description = d['response']['data']['description']
            self.affiliates = [ a['id'] for a in d['response']['data']['affiliates']]
            self.rules = d['response']['data']['rules']
        else :
            raise ValueError(r.json()['response']['errors'])

    def __require_fields__(self, y, z) :
        """
        [DEPRECIATE]
        """
        return (y, z) in [ (x['field'], x['operator']) for x in self.rules]
    
    def setup_value(self, percent: float = None, rate: float = None) -> None:
        """
        Setup CPA/ CPS for this group on offer. Once this method get called, `follow` will be set to 0 for this group.

        Arguments : 
        * percent (float): CPS of group set on offer.
        * rate (float): CPA of grou set on offer.

        Returns : 
        * None
        """
        self.percent = float(percent) if percent else None
        self.rate = float(rate) if rate else None
        self.follow = 0 if percent or rate else 1

    def setup_sub_field(self, sub_value: Any, index: int = 1) -> None: 
        """
        Setup value in sub field in different index of group.

        Arguments : 
        * sub_value (Any) : value to be saved in sub field column.
        * index (int) : index of sub fields

        Returns : 
        * None
        """
        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''DESC GroupSchedule''')
        subs = [x['Field'] for x in cursor.fetchall() if "sub_" in x["Field"]]
        if 'sub_' + str(index) in subs :
            setattr(self, 'sub_' + str(index), sub_value)
        else :
            raise IndexError('[Error] Invalid Sub Index For Passing Value')
        
    def define_configure(self, name: str = None, description: str = None, affiliates: List[str] = None, rules: List[RulesTemplate] = [], inherit_rules: bool = True, limit: int = 250) :
        """
        Setup the configuration of group.

        Argments : 
        * name (str) : name of group, need to be unique.
        * description (str) : description of group.
        * affiliates (List[str]) : Whitelist affiliate_id on HasOffers, set empty to allow every affiliate.
        * rules (List[RulesTemplate]) : List of RulesTemplate.
        * inherit_rules (bool) : determine to stack on current rules or not.
        * limit (int) : limit of number of rules applied to this group.

        Returns : 
        * None
        """
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
        '''
        Sub function of `define_configure`.
        '''
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

def api_req(url, key_value_pair) :
    url += '?'
    params_list = list()
    for kv in key_value_pair :
        params_list += [str(k) + '=' + str(v) for k, v in kv.items()]
    url += '&'.join(params_list)
    r = requests.get(url)
    return r 