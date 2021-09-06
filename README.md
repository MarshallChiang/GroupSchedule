## Introduction
Manipulate events composed of group object, activate and deactive by shifting time window and event priority.
<img src="https://github.com/MarshallChiang/marshallchiang.github.io/blob/master/assets/img/portfolio/fullsize/GroupSchedule_image_1.jpg?raw=true" width=75% height=75%>

## Environment setup
#### Database schema
| Field             | Type                | Null | Key | Default | Extra          |
|-------------------|---------------------|------|-----|---------|----------------|
| id                | int(10) unsigned    | NO   | PRI | NULL    | auto_increment |
| cashflow_group_id | varchar(10)         | YES  |     | NULL    |                |
| offer_id          | varchar(10)         | NO   |     | NULL    |                |
| affiliates        | varchar(100)        | YES  |     | NULL    |                |
| name              | varchar(100)        | YES  |     | NULL    |                |
| description       | text                | YES  |     | NULL    |                |
| percent           | decimal(4,2)        | YES  |     | 0.00    |                |
| rate              | decimal(10,2)       | YES  |     | 0.00    |                |
| default_value     | text                | NO   |     | NULL    |                |
| actived_from      | datetime            | NO   |     | NULL    |                |
| actived_to        | datetime            | NO   |     | NULL    |                |
| created_at        | datetime            | NO   |     | NULL    |                |
| rules             | text                | YES  |     | NULL    |                |
| status            | varchar(20)         | NO   |     | NULL    |                |
| group_type        | varchar(5)          | NO   |     | NULL    |                |
| follow            | tinyint(3) unsigned | NO   |     | NULL    |                |
| sub_1             | text                | YES  |     | NULL    |                |
#### Install libraries
```bash
$ cd path/to/project
$ pip install -r requirements.txt -t .
```
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
|_offer_id_|_int_|_Required_|_id of the offer to initialize the object_|
|_branch_at_|_datetime string_|_Optional_|_fetch specific configuration from given datetime (YYYY-mm-dd HH:MM:SS), if None is given, configuration in the current will be used_|

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
|_actived_from_|_string_|_datetime of this configuration start_|
|_actived_from_|_string_|_datetime of this configuration end_|
|_default_value_|_JSON string_|_default percentage and rate value of this configuration_|
|_groups_|_list_|_list with [groups](#class-groupoffer_id-cashflow_group_id-percent-rate-kwargs) object_|
|_branch_at_|_datetime string_|_datetime specified for fetching specific configuration_|
|_created_at_|_datetime string_|_datetime when this configuration was created_|
|_ptype_|_string_|_payout type of this object as following `'cpa_percentage', 'cpa_rate', 'cpa_both' `_|




---

#### function _group_display(**kwargs)_

search groups inside the cursor by filtering name or value.

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

append new group into cursor with either one of percent or rate assigned at least.

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

assign default percent or rate for object

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

specify start and end datetime for the ready-to-push configuration.

#### Parameters
| Parameter | Type | Required | Description |
|----|----|----|----|
|_actived_from_|_datetime string_|_Required_|_start date of this configuration_|
|_actived_to_|_datetime string_|_Required_|_end date of this configuration_|
|_is_base_|_Boolean_|_Optional_|_setup eternal configuration of this cursor, groups of this _|

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

push the configuration into database

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

deploy configuration loaded on cursor object to server.

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
# group object will be initialized along with cursor object.

group = cursor.groups[0]
```
#### Instance Attributes
| Attributes | Type | Description |
|----|----|----|
|_cashflow_group_id_|_int_|_id of group_|
|_offer_id_|_int_|_id of the initialized offer with group included_|
|_percent_|decimal(4,2)|_percent value of this group_|
|_rate_|_decimal(10,2)_|_decimal value of this group_|
|_db_id_|_int_|_auto increment number in database if cursor is fetched from database_|
|_follow_|_tiny int_|_a binary number to determine whether this group is created by branch as a copy (1) or has been assigned value that need to be prioritized (0)_|
#### _setup_value(**kwargs)_

assign percent of rate value to the group object, group with value assigned by this function will be prioritized and always be fetched when this cashflow_group_id is also existed in the cursor configuration.

#### Parameters
| Parameter | Type | Required | Description |
|----|----|----|----|
|_percent_|_float_|_Optional_|_percent(%) value, default as 0_|
|_rate_|_float_|_Optional_|_rate($) value, default as 0_|

#### Return :
```Python
None
```
