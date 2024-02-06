import json
import requests
import pandas as pd
from pandas import json_normalize
import numpy as np
import boto3
import io 
from io import StringIO
import os  # Import the os module
from datetime import datetime


S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
RPC_ENDPOINT= os.environ.get('MAINNET_ENDPOINT')

LIMIT = 1000

def convert_mist_to_sui(mist_amount):
    try:
        if mist_amount is not None:
            # Convert the string to a float or int before division
            numeric_amount = float(mist_amount)
            return numeric_amount / 1000000000
    except ValueError:
        # Handle cases where conversion is not possible
        return None
    return None

def format_to_two_decimals(value):
    # Convert to numeric value if input is a string representing a number
    if isinstance(value, str):
        try:
            numeric_value = float(value) if '.' in value else int(value)
        except ValueError:
            # If conversion fails, return the original string
            return value
    else:
        numeric_value = 0 if pd.isna(value) else value

    return "{:.2f}".format(float(numeric_value))
    
def query_transaction_blocks(address, limit=1000):
    rpc_url = RPC_ENDPOINT
    headers = {'Content-Type': 'application/json'}
    cursor = None
    
    # Initialize an empty list to store DataFrames for each filter_type
    dfs = []

    for filter_type in ["ToAddress","FromAddress"]:
        query = {
            "filter": {filter_type: address},
            "options": {
                "showInput": True,
                "showRawInput": False,
                "showEffects": True,
                "showEvents": True,
                "showObjectChanges": True,
                "showBalanceChanges": True
            }
        }
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "suix_queryTransactionBlocks",
            "params": [query, cursor, limit, False]
        }

        while True:
            response = requests.post(rpc_url, data=json.dumps(payload), headers=headers).json()
            data = response['result']['data']
            df = pd.json_normalize(data)
            cursor = response['result']['nextCursor']
            has_next_page = response['result']['hasNextPage']
            if not has_next_page:
                break
            payload["params"] = [query, cursor, limit, False]
        
        # Append the DataFrame for each filter_type to the list
        dfs.append(df)

    # Concatenate the DataFrames for each filter_type into a single DataFrame
    result_df = pd.concat(dfs, ignore_index=True)

    # Group by 'effects.executedEpoch'
    grouped_df = result_df.groupby('effects.executedEpoch')

    return grouped_df

def convert_timestamp(timestamp_ms):
    # Ensure the timestamp is in integer format
    timestamp_ms = int(timestamp_ms)
    
    # Convert milliseconds to seconds
    timestamp_seconds = timestamp_ms / 1000.0
    
    # Convert timestamp to datetime
    dt_object = datetime.utcfromtimestamp(timestamp_seconds)
    
    # Format the datetime object as a string
    formatted_date = dt_object.strftime('%Y-%m-%d %H:%M:%S')
    
    return formatted_date

def try_multi_get_past_objects_with_balance_of_sui_coin(df):
    def chunked_requests(request, chunk_size=1000):
        """Yield successive chunk_size chunks from request."""
        for i in range(0, len(request), chunk_size):
            yield request[i:i + chunk_size]

    rpc_url = RPC_ENDPOINT
    headers = {'Content-Type': 'application/json'}

    object_list = df[['objectId', 'version']].drop_duplicates().to_dict('records')
    temp_df = pd.DataFrame(columns=['objectId', 'version', 'balance'])
    results_list = []

    for chunk in chunked_requests(object_list, chunk_size=20):
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sui_tryMultiGetPastObjects",
            "params": [
                [{"objectId": obj['objectId'], "version": str(obj['version'])} for obj in chunk],
                {
                    "showType": True,
                    "showOwner": True,
                    "showPreviousTransaction": True,
                    "showDisplay": False,
                    "showContent": True,
                    "showBcs": False,
                    "showStorageRebate": True
                }
            ]
        }
        response = requests.post(rpc_url, data=json.dumps(payload), headers=headers).json()
        results = response.get('result', [])

        for result in results:
            details = result.get('details', {})
            content = details.get('content', {})
            balance = content.get('fields', {}).get('balance')
            object_id = details.get('objectId')
            version = details.get('version')

            if object_id and version:
                results_list.append({
                    'objectId': object_id,
                    'version': str(version),
                    'balance': balance
                })

    temp_df = pd.DataFrame(results_list)
    df.loc[:, 'version'] = df['version'].astype(str)  # Using .loc for assignment
    df_with_balance = pd.merge(df, temp_df, on=['objectId', 'version'], how='left')
    df_with_balance.rename(columns={'executedEpoch': 'epoch', 'balance': 'liquid_amount'}, inplace=True)

    if 'epoch' in df_with_balance.columns:
        df_with_balance['epoch'] = df_with_balance['epoch'].astype(int)  # or .astype(float) if epoch is a float
        df_with_balance = df_with_balance.sort_values(by='epoch')

    return df_with_balance
    
def try_multi_get_past_objects_with_balance_of_sui_staked(df):
    def chunked_requests(request, chunk_size=1000):
        """Yield successive chunk_size chunks from request."""
        for i in range(0, len(request), chunk_size):
            yield request[i:i + chunk_size]

    rpc_url = RPC_ENDPOINT
    headers = {'Content-Type': 'application/json'}

    object_list = df[['epoch','objectId', 'version']].drop_duplicates().to_dict('records')
    results_list = []

    for chunk in chunked_requests(object_list, chunk_size=20):
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sui_tryMultiGetPastObjects",
            "params": [
                [{"objectId": obj['objectId'], "version": str(obj['version'])} for obj in chunk],
                {
                    "showType": True,
                    "showOwner": True,
                    "showPreviousTransaction": False,
                    "showDisplay": False,
                    "showContent": True,
                    "showBcs": False,
                    "showStorageRebate": False
                }
            ]
        }
        response = requests.post(rpc_url, data=json.dumps(payload), headers=headers).json()
        results = response.get('result', [])

        for result in results:
            details = result.get('details', {})
            content = details.get('content', {})
            fields = content.get('fields', {})
            object_id = details.get('objectId')
            version = details.get('version')
            object_type = details.get('type')
            address_owner = details.get('owner', {}).get('AddressOwner')
            pool_id = fields.get('pool_id')
            principal = fields.get('principal')
            stake_activation_epoch = fields.get('stake_activation_epoch')
             # Get the 'executedEpoch' value from the DataFrame
            epoch = next((record['epoch'] for record in object_list if record['objectId'] == object_id and record['version'] == version), None)
            if object_id and version:
                results_list.append({
                    'objectId': object_id,
                    'version': str(version),
                    'type': object_type,
                    'AddressOwner': address_owner,
                    'pool_id': pool_id,
                    'principal': int(principal),
                    'stake_activation_epoch': stake_activation_epoch,
                    'epoch': epoch
                    
                })
    results_df = pd.DataFrame(results_list)
    # results_df.rename(columns={'stake_activation_epoch': 'epoch'}, inplace=True)
    if 'epoch' in results_df.columns:
        results_df['epoch'] = results_df['epoch'].astype(int)  # or .astype(float) if epoch is a float
        results_df = results_df.sort_values(by='epoch')
    return results_df
    
def process_transactions_by_group(transactions_list, wallet_address):
    # Create empty lists to store dictionaries
    object_changes_sui_data = []
    object_changes_staked_sui_data = []
    balance_changes_data = []
    events= []
     # Keep track of executed_epochs for which 'mutated' has occurred
    mutated_epochs = set()

    for _, transactions_for_epoch in transactions_list:
        for _, transaction in transactions_for_epoch.iterrows():
            # Extract the executedEpoch from the effects if available, otherwise set to [0]
            executed_epochs = transaction['effects.executedEpoch']

            # Check if 'mutated' occurred in this transaction and update the set
            if any(object_change.get('type', '') == 'mutated' for object_change in transaction.get('objectChanges', [])):
                mutated_epochs.add(executed_epochs)

            # Extract the objectChanges array and filter based on conditions from objectChanges
            for object_change in transaction.get('objectChanges', []):
                if (
                    isinstance(object_change, dict) and
                    object_change.get('objectType', '') in ["0x2::coin::Coin<0x2::sui::SUI>"] and
                    object_change.get('owner', {}).get('AddressOwner', '') == wallet_address and 
                    object_change.get('type', '') in ["created","mutated"]
                ):
                    row_dict = {
                        'epoch': executed_epochs,
                        'objectId': object_change.get('objectId', ''),
                        'version': object_change.get('version', ''),
                        'digest': object_change.get('digest', ''),
                        'objectType': object_change.get('objectType', ''),
                        'type': object_change.get('type', ''),
                    }
                    object_changes_sui_data.append(row_dict)

                elif(
                    isinstance(object_change, dict) and
                    object_change.get('objectType', '') in ["0x3::staking_pool::StakedSui"] and
                    object_change.get('owner', {}).get('AddressOwner', '') == wallet_address and 
                    object_change.get('type', '') in ["created","mutated"]):
                        row_dict = {
                            'epoch': executed_epochs,
                            'objectId': object_change.get('objectId', ''),
                            'version': object_change.get('version', ''),
                            'digest': object_change.get('digest', ''),
                            'objectType': object_change.get('objectType', ''),
                            'type': object_change.get('type', ''),
                        }
                        object_changes_staked_sui_data.append(row_dict)
            
             # Extract the balanceChanges array and filter based on conditions from balanceChanges
            for balance_change in transaction.get('balanceChanges', []):
                # Check if the balanceChange is a dictionary and meets the filter criteria for balanceChange
                if isinstance(balance_change, dict) and balance_change.get('owner', {}).get('AddressOwner', '') == wallet_address and balance_change.get('coinType', {}) == "0x2::sui::SUI":
                    # Extract relevant information
                    row_dict = {
                        'epoch': executed_epochs,
                        'amount': int(balance_change.get('amount', 0)),
                        'token': balance_change.get('coinType', ''),
                    }
                    balance_changes_data.append(row_dict)
                    
            # Extract the 'parsedJson' array and filter based on conditions from events
            for event in transaction.get('events', []):
                event_type = event.get('type', '')
                if event.get('sender', '') == wallet_address and event_type in ["0x3::validator::StakingRequestEvent", "0x3::validator::UnstakingRequestEvent"]:
                    row_dict = {
                        'epoch': executed_epochs,
                        'type': event_type,
                        'digest': event.get('id', {}).get('txDigest', ''),
                        'eventSeq': event.get('id', {}).get('eventSeq', ''),
                    }

                    parsed_json = event.get('parsedJson', {})
                    if event_type == "0x3::validator::StakingRequestEvent":
                        row_dict.update({
                            'amount': parsed_json.get('amount', ''),
                            'parsed_epoch': parsed_json.get('epoch', ''),
                            'pool_id': parsed_json.get('pool_id', ''),
                            'staker_address': parsed_json.get('staker_address', ''),
                            'validator_address': parsed_json.get('validator_address', ''),
                        })
                    elif event_type == "0x3::validator::UnstakingRequestEvent":
                        row_dict.update({
                            'pool_id': parsed_json.get('pool_id', ''),
                            'principal_amount': parsed_json.get('principal_amount', ''),
                            'reward_amount': parsed_json.get('reward_amount', ''),
                            'stake_activation_epoch': parsed_json.get('stake_activation_epoch', ''),
                            'staker_address': parsed_json.get('staker_address', ''),
                            'unstaking_epoch': parsed_json.get('unstaking_epoch', ''),
                            'validator_address': parsed_json.get('validator_address', ''),
                        })

                    # print("Appending event to the events list\n", row_dict)
                    events.append(row_dict)



    # Create DataFrames from the lists of dictionaries
    object_changes_sui_df = pd.DataFrame(object_changes_sui_data)
    object_changes_staked_sui_df = pd.DataFrame(object_changes_staked_sui_data)
    balance_changes_data_df = pd.DataFrame(balance_changes_data)
    event_df = pd.DataFrame(events)
    # Assuming 'object_changes_sui_df' is the DataFrame containing the object_changes_sui_data
    # Filter out 'created' objects where there is at least one 'mutated' object in the same executed_epochs
    filtered_object_changes_sui_df = object_changes_sui_df[
        ~((object_changes_sui_df['type'] == 'created') & (object_changes_sui_df['epoch'].isin(mutated_epochs)))
    ].copy()
    # print(filtered_object_changes_sui_df)
    return filtered_object_changes_sui_df, object_changes_staked_sui_df, balance_changes_data_df, event_df

def query_latest_sui_system_state():

    rpc_url = RPC_ENDPOINT
    headers = {'Content-Type': 'application/json'}
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getLatestSuiSystemState",
        "params": [ None, 1000, False]
    }

    response = requests.post(rpc_url, json=payload, headers=headers).json()
    data = response.get('result', {})
    df = pd.json_normalize(data)
    return pd.DataFrame(df)
    
def query_validator_epoch_info_events_with_criteria(desired_epoch, cursor=None, limit=1000):

    url = RPC_ENDPOINT
    headers = {'Content-Type': 'application/json'}
    query = {"MoveEventType": "0x3::validator_set::ValidatorEpochInfoEventV2"}
    data_list = []

    while True:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "suix_queryEvents",
            "params": [query, cursor, limit, False]
        }
        response = requests.post(url, json=payload, headers=headers).json()
        data = response.get('result', {}).get('data', [])

        if not data:
            break

        # Check if the desired epoch is found
        if int(data[-1].get('parsedJson', {}).get('epoch')) == desired_epoch:
            data_list.extend(data)
            break
        else:
            data_list.extend(data)
            cursor = response.get('result', {}).get('nextCursor')
            has_next_page = response.get('result', {}).get('hasNextPage', False)

            if not has_next_page:
                break
    # Extract the specified columns from the data and create a DataFrame
    result_df = pd.DataFrame([
        {
            'parsedJson.epoch': event['parsedJson'].get('epoch'),
            'parsedJson.validator_address': event['parsedJson'].get('validator_address'),
            'parsedJson.commission_rate': event['parsedJson'].get('commission_rate'),
            'parsedJson.pool_token_exchange_rate.pool_token_amount': event['parsedJson']['pool_token_exchange_rate'].get('pool_token_amount'),
            'parsedJson.pool_token_exchange_rate.sui_amount': event['parsedJson']['pool_token_exchange_rate'].get('sui_amount'),
            'parsedJson.validator_address': event['parsedJson'].get("validator_address")
        }
        for event in data_list
    ])

    return result_df

def get_dynamic_fields(parent_object_id):
        
        rpc_url = RPC_ENDPOINT
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "suix_getDynamicFields",
            "params": [parent_object_id]
        }
        response = requests.post(rpc_url, data=json.dumps(payload), headers=headers).json()
        normalised_data = json_normalize(response['result']['data'])
        return pd.DataFrame(normalised_data)

def get_object_by_id(object_id):

        rpc_url = RPC_ENDPOINT
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sui_getObject",
            "params": [object_id, {
                        "showType": True,
                        "showOwner": True,
                        "showPreviousTransaction": True,
                        "showDisplay": False,
                        "showContent": True,
                        "showBcs": False,
                        "showStorageRebate": True
                }]
        }
        response = requests.post(rpc_url, data=json.dumps(payload), headers=headers).json()
        normalised_data = json_normalize(response['result']['data'])
        return pd.DataFrame(normalised_data)

def get_validator_id_for_inactive_pool(pool_id, sui_system_state):
    # get inactive pools_id
    inactive_pools = get_dynamic_fields(sui_system_state['inactivePoolsId'])
    if not inactive_pools.empty:
        print("inactive_pools:\n", inactive_pools)
    validator_wrapper = None
    for item in inactive_pools:
        if item['name']['value'] == pool_id:
            validator_wrapper = item['objectId']
            break
    if validator_wrapper is None:
        raise Exception(f"Could not find validator wrapper for pool {pool_id}")
    
    validator_wrapper_object = get_object_by_id(validator_wrapper)
    validator_dynamic_fields_id = validator_wrapper_object['content']['fields']['value']['fields']['inner']['fields']['id']['id']
    print("validator_dynamic_fields_id:\n", validator_dynamic_fields_id)
    validator_object = get_dynamic_fields(validator_dynamic_fields_id)
    validator_object_id = validator_object[0]['objectId']
    print("validator_object_id:\n", validator_object_id)
    validator_object = get_object_by_id(validator_object_id)
    validator_address = validator_object['content']['fields']['value']['fields']['metadata']['fields']['sui_address']
    print("validator_address:\n", validator_address)
    return validator_address
    
def calculate_rewards(system_state_df, epoch_validator_event_df, principal, pool_id, active_epoch, target_epoch):

    validator_id = next(
        (validator['suiAddress'] for validators_list in system_state_df['activeValidators'] for validator in validators_list if validator['stakingPoolId'] == pool_id),
        None
    )

    if not validator_id:
        validator_id = get_validator_id_for_inactive_pool(pool_id, system_state_df)


    rate_at_activation_epoch = 1
    rate_at_target_epoch = None


    for epoch, validator_address in [(active_epoch, validator_id), (target_epoch, validator_id)]:
        filtered_df = epoch_validator_event_df.loc[
        (epoch_validator_event_df['parsedJson.epoch'] == str(epoch)) &
        (epoch_validator_event_df['parsedJson.validator_address'] == validator_address)
    ]

        if not filtered_df.empty:
            
            pool_token_amount = int(filtered_df['parsedJson.pool_token_exchange_rate.pool_token_amount'].iloc[0])
            sui_amount = int(filtered_df['parsedJson.pool_token_exchange_rate.sui_amount'].iloc[0])
            validator_address = filtered_df['parsedJson.validator_address'].iloc[0] 
  
            rate = pool_token_amount / sui_amount
            if epoch == active_epoch:
                rate_at_activation_epoch = rate
            if epoch == target_epoch:
                rate_at_target_epoch = rate

            if rate_at_activation_epoch and rate_at_target_epoch:
                break

        
    rate_at_target_epoch = 1 if rate_at_target_epoch is None else rate_at_target_epoch
    estimated_reward = max(0, ((rate_at_activation_epoch / rate_at_target_epoch) - 1.0) * principal)
    result_dict = {
        'rate_at_activation_epoch': rate_at_activation_epoch,
        'rate_at_target_epoch': rate_at_target_epoch,
        'estimated_reward': estimated_reward,
        'validator_id': validator_id,
        'pool_id': pool_id,
    }

    return result_dict

def calculate_rewards_for_address(epoch_validator_event_df, start_epoch, end_epoch, staked_sui_objs_df, use_previous_epoch=False):
    sui_system_state = query_latest_sui_system_state()
    estimated_rewards = 0
    staked_sui = 0

    for index, staked_sui_obj in staked_sui_objs_df.iterrows():
        if use_previous_epoch:
            activation_epoch = max(int(staked_sui_obj['stake_activation_epoch']), end_epoch - 1, 0)
        else:
            activation_epoch = max(int(staked_sui_obj['stake_activation_epoch']), start_epoch)

        result = calculate_rewards(sui_system_state, epoch_validator_event_df,
                                    staked_sui_obj['principal'],
                                    staked_sui_obj['pool_id'],
                                    activation_epoch,
                                    end_epoch)
 
        estimated_rewards += result['estimated_reward']
        staked_sui += staked_sui_obj['principal']

    return staked_sui, estimated_rewards
    
def generate_epoch_data_frame(start_epoch, end_epoch, sui_coin_df, sui_staked_df, balance_changes_df, epoch_validator_event_df, event_df):
    epochs = range(start_epoch, end_epoch + 1)
    df = pd.DataFrame(epochs, columns=['epoch'])

    df['liquid_amount'] = 0.0  
    df['staked_amount'] = 0.0  
    df['reward_amount'] = 0.0 
    previous_liquid = 0
    previous_staked = 0
    previous_estimated_rewards = 0


    balance_changes_df['epoch'] = balance_changes_df['epoch'].astype(int)

    
    balance_sum_by_epoch = balance_changes_df.groupby('epoch')['amount'].sum()

   
    event_df['epoch'] = event_df['epoch'].astype(int)
    

    
    sui_staked_df['epoch'] = sui_staked_df['epoch'].astype(int)
   
  
    
    sui_staked_df_by_epoch = sui_staked_df.groupby('epoch').agg({
    'principal': 'sum',
    'objectId': 'first',  
    'version': 'first',  
    'type': 'first',     
    'AddressOwner': 'first',  
    'pool_id': 'first',   
    'stake_activation_epoch': 'first'  
        }).reset_index()
  

    for i, epoch in enumerate(df['epoch']):

   
        sui_staked_df_by_current_epoch = sui_staked_df[sui_staked_df['epoch'] <= epoch].groupby('epoch').agg({
            'principal': 'sum',
            'objectId': 'first', 
            'version': 'first',  
            'type': 'first',     
            'AddressOwner': 'first',  
            'pool_id': 'first',   
            'stake_activation_epoch': 'first'  
        }).reset_index()

        key_liquid_amount_epoch = sui_coin_df['epoch'] == epoch
        liquid_amount_epoch = sui_coin_df.loc[key_liquid_amount_epoch, 'liquid_amount']
       
        key_staked_amount_epoch = sui_staked_df_by_epoch['epoch'] == epoch
        
        staked_amount_epoch = sui_staked_df_by_epoch.loc[key_staked_amount_epoch, 'principal']


  
        key_event_epoch = event_df['epoch'] == epoch
        if key_event_epoch.any():
          
            staking_condition = (event_df['type'] == "0x3::validator::StakingRequestEvent")
            
        
            if staking_condition.any():
                selected_events = event_df.loc[key_event_epoch & staking_condition, ['amount', 'type', 'epoch', 'pool_id', 'staker_address', 'validator_address']]
            else:
                selected_events = event_df.loc[key_event_epoch, ['pool_id', 'type', 'principal_amount', 'reward_amount', 'stake_activation_epoch', 'staker_address', 'unstaking_epoch', 'validator_address']]
       

        
        if epoch == 0:
            df.at[i, 'liquid_amount'] = float(liquid_amount_epoch.values[0]) if not liquid_amount_epoch.empty else 0
            df.at[i, 'staked_amount'] = float(staked_amount_epoch.values[0]) if not staked_amount_epoch.empty else 0
        else:
            
            previous_liquid = int(df.at[i - 1, 'liquid_amount'])
            balance_amount_found_epoch = balance_sum_by_epoch.get(epoch, None)

            if balance_amount_found_epoch is not None:
                
                df.at[i, 'liquid_amount'] = previous_liquid + balance_amount_found_epoch
            else:
                
                df.at[i, 'liquid_amount'] = previous_liquid
            
            previous_staked = int(df.at[i - 1, 'staked_amount'])
            currrent_staked_amount_found_epoch = sui_staked_df_by_epoch.loc[sui_staked_df_by_epoch['epoch'] == epoch, 'principal'].values[0] if not sui_staked_df_by_epoch[sui_staked_df_by_epoch['epoch'] == epoch].empty else None
            
            
            if key_event_epoch.any() and selected_events['type'].iloc[0] == "0x3::validator::UnstakingRequestEvent":
                
                unstaking_amount = selected_events['principal_amount'].iloc[0]
                
                if not pd.isna(unstaking_amount):
                    df.at[i, 'staked_amount'] = max(previous_staked - int(unstaking_amount), 0)
                   
            
            elif key_event_epoch.any() and selected_events['type'].iloc[0] == "0x3::validator::StakingRequestEvent":
                staking_amount = selected_events['amount'].iloc[0]
                df.at[i, 'staked_amount'] = previous_staked + int(staking_amount)
               
            else:
                
                df.at[i, 'staked_amount'] = previous_staked 

            sui_staked, estimated_rewards = calculate_rewards_for_address(epoch_validator_event_df, start_epoch, epoch, sui_staked_df_by_current_epoch, False)
            
            if estimated_rewards == 0:
               
                df.at[i, 'reward_amount'] = previous_estimated_rewards
            else:
                
                df.at[i, 'reward_amount'] = estimated_rewards

                

        previous_liquid = int(df.at[i, 'liquid_amount'])
        previous_staked = int(df.at[i, 'staked_amount'])
        previous_estimated_rewards = int(df.at[i, 'reward_amount'])


    
    columns_to_convert = ['liquid_amount', 'staked_amount', 'reward_amount']
    for column in columns_to_convert:
        df[column] = df[column].apply(convert_mist_to_sui).apply(format_to_two_decimals)

    return df

def lambda_handler(event, context):
    
    try:
        body = json.loads(event['body'])
        address = body.get('wallet_address')
        start_epoch = body.get('start_epoch')
        end_epoch = body.get('end_epoch')
    
        
        if address is None:
            return {"statusCode": 400, "body": json.dumps("Address parameter is missing from sui_staking_rewards_cal_lambda")}
    except (AttributeError, TypeError):
        return {"statusCode": 400, "body": json.dumps("Invalid format in the request body!!!")}

    transactions = query_transaction_blocks(address)
    
    epoch_validator_event_df = query_validator_epoch_info_events_with_criteria(end_epoch)
    object_changes_sui_df, object_changes_staked_sui_df, balance_changes_df, event_df = process_transactions_by_group(transactions, address)
    sui_coin_df = try_multi_get_past_objects_with_balance_of_sui_coin(object_changes_sui_df)
    sui_staked_df = try_multi_get_past_objects_with_balance_of_sui_staked(object_changes_staked_sui_df)
    final_report_df = generate_epoch_data_frame(start_epoch, end_epoch, sui_coin_df, sui_staked_df, balance_changes_df, epoch_validator_event_df, event_df)
    aggregated_transactions = transactions.count()
    records_found = not aggregated_transactions.empty

    if not records_found:
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Record not found for the given address.", "records_found": False})
        }

    excel_filename = f"{address}_staking_rewards_statement.xlsx"  # Excel filename based on the address
    excel_buffer = io.BytesIO()
    final_report_df.to_excel(excel_buffer, index=False, engine='openpyxl')
    excel_buffer.seek(0)
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=excel_filename, Body=excel_buffer.getvalue())
    presigned_url = s3_client.generate_presigned_url('get_object',
                                                     Params={'Bucket': S3_BUCKET_NAME, 'Key': excel_filename},
                                                     ExpiresIn=3600)  

    df_json = final_report_df.to_json(orient='records')
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
        },
        "body": json.dumps({
            "download_url": presigned_url, 
            "records_found": True,
            "data_frame": json.loads(df_json)  
        })
    }
  