# Monzo Take-Home Test
Submitted by: Diego Giraldo

## Overview

The objective of this project was to create two data models as part of the Monzo Take Home Test. These will be presented in this README file. 

The BigQuery materialised objects have been created in the schema `DG_take_home_test`, and the tables are prefixed with `DG_`. 

The objects were created via this dbt project with the [profile](monzo_take_home_test/profiles.yml) and [configurations](monzo_take_home_test/dbt_project.yml) in the respective files.

---

## Task 1: Accounts

The objective of this task was to create a comprehensive table that represents all the different accounts at Monzo.

### Model: [`dim_accounts`](monzo_take_home_test/models/core/dim_account.sql)

#### Description

The `dim_accounts` model captures key attributes related to each account.

#### Dependencies

The `dim_accounts` model is constructed using data from two staging tables: `stg_account_event_history` and `stg_account_hub`.

1. [**stg_account_event_history**](monzo_take_home_test/models/staging/stg_account_event_history.sql):
   - This staging table contains historical event data related to account changes. It records all events for each account, such as when an account is created, closed, or reopened. The data in this table is used to calculate the account status in the `dim_accounts` model, ensuring that the event timestamps are used to determine the status of an account. 
   - **Source tables**:
     - `monzo_datawarehouse.account_created`
     - `monzo_datawarehouse.account_closed`
     - `monzo_datawarehouse.account_reopened`

2. [**stg_account_hub**](monzo_take_home_test/models/staging/stg_account_hub.sql):
   - This staging table serves as a central hub for account data. It includes attributes for each account, such as the account type and associated user information. The data in this table is used to enrich the information in the `dim_accounts` model, ensuring that each account is represented with all relevant details.
   - **Source table**:
     - `monzo_datawarehouse.account_created`

### Schema

The schema for the `dim_accounts` model is as follows:

| Column Name         | Description                                              |
|---------------------|----------------------------------------------------------|
| account_id_hashed   | Unique identifier for each account.                      |
| user_id_hashed      | Unique identifier for the user associated with the account. |
| account_type        | Type of account (e.g., checking, savings).              |
| created_ts          | Timestamp when the account was created.                 |
| closed_ts           | Timestamp when the account was closed.                  |
| reopened_ts         | Timestamp when the account was reopened.                |
| account_status      | Current status of the account (open, closed, reopened). |

For the complete schema, refer to the [full YAML file](monzo_take_home_test/models/core/_schema.yml).

### Materialised Object

The materialised object for this model in BigQuery can be found here:
```
analytics-take-home-test.DG_take_home_test.DG_dim_account
```
---

### Tests

To ensure the accuracy and reliability of the `dim_accounts` model, the following tests have been implemented:

1. **Uniqueness**: Verify that each `account_id_hashed` is unique within the model to ensure that no duplicate accounts exist.
2. **Not Null**: Confirm that key fields (e.g., `user_id_hashed`, `account_type`, `created_ts`) are not null to maintain dimension integrity.
3. **Date Consistency**: Check that the `closed_ts` is always later than the `created_ts`, ensuring that an account cannot be closed before it is created.
4. **Account Status Logic**: Validate the logic for determining `account_status` to ensure it accurately reflects whether an account is 'open', 'closed', or 'reopened' based on event history.
5. **Referential Integrity**: Ensure that each `account_id_hashed` in the `dim_accounts` model exists in the `account_created` table, confirming that all accounts are valid and preventing orphaned records.

Tests are configured at a column level in the [schema.yml](monzo_take_home_test/models/core/_schema.yml) file, with additional custom tests created in the [macros](monzo_take_home_test/macros/tests) folder.

---
## Task 2: 7-Day Active Users Metric

The objective of this task was to create a model that calculates the 7-day active users metric for Monzo.

### Model: [`fact_7d_active_users`](monzo_take_home_test/models/core/fact_7d_active_users.sql)

#### Description

The `fact_7d_active_users` model calculates the 7-day active users metric based on user transactions and account statuses. It provides insights into user activity by measuring how many users had transactions in the past 7 days and how many of those users had at least one open account on the same date.

#### Dependencies

The `fact_7d_active_users` model is constructed using data from three staging and dimensional tables:

1. [**stg_user_transactions**](monzo_take_home_test/models/staging/stg_user_transactions.sql):
   - This staging table aggregates user transaction data, recording the number of transactions for each user on each date. It serves as the basis for calculating active users based on their transaction history.
   - **Source table**:
     - `monzo_datawarehouse.account_transactions`

3. [**stg_account_event_history**](monzo_take_home_test/models/staging/stg_account_event_history.sql):
   - This staging table logs all historical events related to account changes, such as creation, closure, and reopening of accounts. It is used for determining the status of accounts over time.
   - **Source tables**:
     - `monzo_datawarehouse.account_created`
     - `monzo_datawarehouse.account_closed`
     - `monzo_datawarehouse.account_reopened`

3. [**dim_account**](monzo_take_home_test/models/core/dim_account.sql):
   - This core table contains the current state of accounts, including attributes such as account status and timestamps for created, closed, and reopened events. It is used to associate each account event with a user id (and future account dimensions in the future)

### Schema

The schema for the `fact_7d_active_users` model is as follows:

| Column Name            | Description                                               |
|------------------------|-----------------------------------------------------------|
| date_key               | The date for which the metric is calculated.             |
| active_users_count     | The number of users who had a transaction in the last 7 days. |
| open_users_count       | The number of users with at least one open account on the date. |
| `7d_active_users`      | The ratio of active users to users with open accounts for that date.   |

For the complete schema, refer to the [full YAML file](monzo_take_home_test/models/core/_schema.yml).

### Materialised Object

The materialised object for this model in BigQuery can be found here:
```
analytics-take-home-test.DG_take_home_test.DG_fact_7d_active_users
```