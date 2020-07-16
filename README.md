## Introduction
Manipulate events composed of group object, activate and deactive by shifting time window and event priority.

## Environment setup



## Available Methods

* [OfferObject](#class-offergroupcursoroffer_id-kwargs)
  * [attributes](#instance-attributes)
  * [group_display()](#function-group_displaykwargs)
  * [group_append()](#function-group_appendcashflow_group_id-kwargs)
  * [group_remove()](#function-group_removecashflow_group_id)
  * [setup_default_value()](#function-setup_default_valuekwargs)
  * [setup_period()](#function-setup_periodkwargs)
  * [push_cursor()](#function-push_cursorkwargs)
  * [deploy_cursor()](#function-deploy_cursor)
* [GroupObject](#class-groupoffer_id-cashflow_group_id-percent-rate-kwargs)

### class _OfferGroupCursor(offer_id, **kwargs)_

#### Parameters :
| Parameter | Type | Required | Description |
|----|----|----|----|
|_offer_id_|_int_|_Required_|_id of the offer which will be used to initialize the object with attributes_|
|_branch_at_|_datetime string_|_Optional_|_to get the offer configure in specific datetime (YYYY-mm-dd HH:MM:SS), if None will use current datetime_|

```Python
from GroupSchedule import OfferGroupCursor

# get offer 1544 current setting.
cursor = OfferGroupCursor(1544)

# get offer 1544 setting at 2019-01-01 18:00:00
cursor = OfferGroupCursor(1544, branch_at='2019-01-01 18:00:00')
```
#### Return :
```Python
None
```
#### Instance Attributes
| Attributes | Type | Description |
|----|----|----|
|_offer_id_|_int_|_id of the initialized offer_|
|_actived_from_|_string_|_datetime of this configure start_|
|_actived_from_|_string_|_datetime of this configure end_|
|_default_value_|_JSON string_|_default percentage and rate value of current or branch_at moment_|
|_groups_|_list_|_list with [groups](#class-groupoffer_id-cashflow_group_id-percent-rate-kwargs) object_|
|_branch_at_|_datetime string_|_datetime of this object configure_|
|_created_at_|_datetime string_|_datetime of creating this object_|
|_ptype_|_string_|_payout type of this object as following `'cpa_percentage', 'cpa_rate', 'cpa_both' `_|




---

#### function _group_display(**kwargs)_

Search groups by names or value and return dictionary information.

#### Parameters :
| Parameter | Type | Required | Description |
|----|----|----|----|
|_text_filter_|_string_|_Optional_|_key for group name search_|
|_value_filter_|_string_|_Optional_|_key for group value search_|
|_return_object_|_string_|_Optional_|_return list with eligible groups_|

```Python
groups = cursor.group_display(text_filter='AU')
```

#### Return :
```Python
# dictionary in list
[{'name': 'AU',
  'description': 'AU Group',
  'affiliates': [],
  'rules': [{'id': '1234',
    'cashflow_group_id': '2345',
    'field': 'field7',
    'operator': 'IN',
    'value': ['rule_value_1'],
    'negate': '0'},
   {'id': '4567',
    'cashflow_group_id': '2345',
    'field': 'field8',
    'operator': 'IN',
    'value': ['rule_value_2'],
    'negate': '1'}],
  'cashflow_group_id': '2345',
  'offer_id': 1544,
  'percent': '10',
  'rate': None,
  'follow': 1}]
  
  # group object in list if return_object=True
  [GroupSchedule.Group]
```
---

#### function _group_append(cashflow_group_id, **kwargs)_

add group into cursor object and define its value, at least one type of value need to be defined.

#### Parameters :
| Parameter | Type | Required | Description |
|----|----|----|----|
|_cashflow_group_id_|_int_|_Required_|_id of cashflowgroup object_|
|_percent_|_float_|_Optional_|_percent(%) type value_|
|_rate_|_float_|_Optional_|_rate($) type value_|

```Python
cursor.group_append(2000, percent=5.5, rate=30)
```
#### Return :
```Python
None
```
---

#### function _group_remove(cashflow_group_id)_

remove group from cursor object.

#### Parameters :
| Parameter | Type | Required | Description |
|----|----|----|----|
|_cashflow_group_id_|_int_|_Required_|_id of cashflowgroup object_|

```Python
cursor.group_remove(2000)
```
#### Return :
```Python
None
```
---

#### function _setup_default_value(**kwargs)_

setup default percent or rate for object

#### Parameters
| Parameter | Type | Required | Description |
|----|----|----|----|
|_percent_|_float_|_Optional_|_percent(%) value, default as 0_|
|_rate_|_float_|_Optional_|_rate($) value, default as 0_|

```Python
cursor.setup_default_value(percent=10, rate=5.5)
```
#### Return :
```Python
#JSON format string
'{
  "max_payout": 5.5, 
  "default_payout": 5.5, 
  "max_percent_payout": 10, 
  "percent_payout": 10
  }'
```
---

#### function _setup_period(**kwargs)_

setup start and end datetime for this ready-to-push configure

#### Parameters
| Parameter | Type | Required | Description |
|----|----|----|----|
|_actived_from_|_datetime string_|_Required_|_start date of this configure_|
|_actived_to_|_datetime string_|_Required_|_end date of this configure_|
|_is_base_|_Boolean_|_Optional_|_define an enternal configure for others fallback to, default as `False`. if `True`, actived_from and actived_to will be auto adjust into min and max datetime value_|

```Python
# set specific time range for this configure
cursor.setup_period(actived_from='2019-01-01 00:00:00', actived_to='2019-01-31 23:59:59')

# set baseline for this object to have every configure fallback to
cursor.setup_period(is_base=True)
```
#### Return :
```Python
None
```
---

#### fuction _push_cursor(**kwargs)_

push the configure into mysql database

#### Parameters
| Parameter | Type | Required | Description |
|----|----|----|----|
|_utc_| _int_ | _Optional_ | _push the configuration into database and wait for activation_ |

```Python
cursor.push_cursor()
```

#### Return :
```Python
None
```
---
#### function _deploy_cursor()_

deploy the configure into platform, prioritize configure by `created_at`, the latest will be used for deployment

```Python
cursor.deploy_cursor()
```
#### Return :
```Python
None
```
---
### class _Group(offer_id, cashflow_group_id, percent, rate, **kwargs)_

```Python
# we execute Group object from OfferCursor object, we don't really create one

# index directly from OfferCursor attribute
group = cursor.groups[0]

# get from group_display
groups = cursor.group_display(text_filter='A', return_object=True)
group = groups[0]
```
#### _setup_value(**kwargs)_

setup percent or rate value for per group. for every value declared group, would be out of job prioritize linkage when it's  the highest and proceed to the actived_to

#### Parameters
| Parameter | Type | Required | Description |
|----|----|----|----|
|_percent_|_float_|_Optional_|_percent(%) value, default as 0_|
|_rate_|_float_|_Optional_|_rate($) value, default as 0_|
