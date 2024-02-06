import json
import requests
import pandas as pd
import numpy as np
import boto3
import io 
from io import StringIO
import os  # Import the os module



sui_usd_bucket_name = os.environ.get('SUI_USD_BUCKET_NAME')
sui_usd_file_name = os.environ.get('SUI_USD_FILE_NAME')
indexer_endpoint = os.environ.get('INDEXER_ENDPOINT')

def query_stakes(wallet_address):
    """
    This function queries stakes for a given wallet address.

    Parameters:
    wallet_address (str): The wallet address to query stakes for.

    Returns:
    dict: A dictionary containing the stakes for the given wallet address. If an error occurs during the request, None is returned.
    """
    headers = {'Content-Type': 'application/json'}

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getStakes",
        "params": [wallet_address]
    }

    try:
        response = requests.post(indexer_endpoint, json=payload, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad requests
        data = response.json()

        return data.get('result', {})
    except requests.RequestException as e:
        print(f"Error querying stakes: {e}")
        return None

def flatten_stake_data(stake_data):
    """
    This function takes a list of stake data, flattens it, and returns the flattened data.
    
    Parameters:
    stake_data (list): A list of stake data to be flattened.

    Returns:
    list: A list of dictionaries, where each dictionary represents a flattened stake.
    """
    flattened_data = []
    for validator in stake_data:
        validator_address = validator.get('validatorAddress', '')
        staking_pool = validator.get('stakingPool', '')

        for stake in validator.get('stakes', []):
            flat_stake = {
                'Validator Address': validator_address,
                'Staking Pool ID': staking_pool,
                'Staked Sui Object ID': stake.get('stakedSuiId', ''),
                'Stake Request Epoch': stake.get('stakeRequestEpoch', ''),
                'Stake Active Epoch': stake.get('stakeActiveEpoch', ''),
                'Principal': stake.get('principal', ''),
                'Status': stake.get('status', ''),
                'Estimated Reward': stake.get('estimatedReward', '')
            }
            flattened_data.append(flat_stake)

    return flattened_data

def query_transaction_blocks(address, cursor=None, limit=1000, descending_order=False):
    """
    This function queries transaction blocks for a given address.

    Parameters:
    address (str): The address to query transaction blocks for.
    cursor (str, optional): The cursor to start the query from. Defaults to None.
    limit (int, optional): The maximum number of transaction blocks to return. Defaults to 1000.
    descending_order (bool, optional): Whether to return the transaction blocks in descending order. Defaults to False.

    Returns:
    list: A list of transaction blocks for the given address.
    """
    query = {
        "filter": {"FromAddress": address},
        "options": {
            "showInput": True, "showRawInput": False, "showEffects": True,
            "showEvents": True, "showObjectChanges": True, "showBalanceChanges": True
        }
    }
    headers = {'content-type': 'application/json'}
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "suix_queryTransactionBlocks",
        "params": [query, cursor, limit, descending_order]
    }
    transactions = []

    while True:
        response = requests.post(indexer_endpoint, data=json.dumps(
            payload), headers=headers).json()
        data = response['result']['data']
        transactions.extend(data)
        cursor = response['result']['nextCursor']
        if not response['result']['hasNextPage']:
            break
        payload["params"] = [query, cursor, limit, descending_order]
    return transactions

def flatten_transaction_data(transaction_data, stakes_df, address):
    """
    This function takes a list of transaction data, flattens it, and returns the flattened data.
    
    The function extracts various details from the transaction data such as timestamp, transaction type, status, 
    address, amount, transaction fee, net transaction value, validator address, and staking pool ID. 
    It also calculates the transaction fee and net transaction value based on the balance changes and gas used details.

    Parameters:
    transaction_data (list): A list of transaction data to be flattened.

    Returns:
    list: A list of dictionaries, where each dictionary represents a flattened transaction.
    """
    flattened_data = []
    for transaction_record in transaction_data:

        validator_address = ''
        staking_pool_id = ''
        principal_amount = 0
        reward_amount = 0
        stake_object_id = None
        events = transaction_record.get('events', [])    
        for event in events:
            if 'parsedJson' in event:
                parsed_json = event['parsedJson']
                if 'validator_address' in parsed_json:
                    validator_address = parsed_json['validator_address']
                if 'pool_id' in parsed_json:
                    staking_pool_id = parsed_json['pool_id']            

                if event.get('type') == "0x3::validator::UnstakingRequestEvent":
                    principal_amount = int(parsed_json.get('principal_amount', 0))
                    reward_amount = int(parsed_json.get('reward_amount', 0))
                    break
        if any(event.get('type') in ["0x3::validator::UnstakingRequestEvent", "0x3::validator::StakingRequestEvent"] for event in events):
            flat_transaction = {
                'Unix Timestamp (ms)': transaction_record.get('timestampMs', ''),
                'Human-Readable Timestamp': get_human_readable_timestamp(transaction_record.get('timestampMs', '')),
                'Digest': transaction_record.get('digest', ''),
                'Transaction Type': '',
                'Status': transaction_record.get('effects', {}).get('status', {}).get('status', ''),
                'Explorer URL': '',
                'Address': address,
                'Amount': '',
                'Transaction Fee': '',
                'Net Transaction Value': '',
                'Validator Address': validator_address,
                'Staking Pool ID': staking_pool_id,
                'Principal Amount': principal_amount,
                'Reward Amount': reward_amount,
                'Stake Object Id': stake_object_id,
            }
            transactions = transaction_record.get('transaction', {}).get(
                'data', {}).get('transaction', {}).get('transactions', [])
            for tx in transactions:
                if 'MoveCall' in tx:
                    move_call = tx['MoveCall']
                    function_name = move_call.get('function', '')
                    if function_name == "request_add_stake":
                        flat_transaction['Transaction Type'] = 'Staking'

                        stake_info = stakes_df.query(f"`Validator Address` == '{validator_address}'")
                        if not stake_info.empty:                          
                            principal_amount = int(stake_info.iloc[0].get('Principal', 0))
                            reward_amount = int(stake_info.iloc[0].get('Estimated Reward', 0))
                            stake_object_id = stake_info.iloc[0].get('Staked Sui Object ID', None)
                            flat_transaction['Principal Amount'] = principal_amount
                            flat_transaction['Reward Amount'] = reward_amount
                            flat_transaction['Stake Object Id'] = stake_object_id
                    elif function_name == "request_withdraw_stake":
                        flat_transaction['Transaction Type'] = 'Withdraw'

            if flat_transaction['Digest']:
                flat_transaction[
                    'Explorer URL'] = f"https://suiscan.xyz/mainnet/tx/{flat_transaction['Digest']}"

            balance_changes = transaction_record.get('balanceChanges', [])

            gas_used = transaction_record.get('effects', {}).get('gasUsed', {})
            computation_cost = int(gas_used.get('computationCost', 0))
            storage_cost = int(gas_used.get('storageCost', 0))
            storage_rebate = int(gas_used.get('storageRebate', 0))


            transaction_fee = computation_cost + storage_cost - storage_rebate
            flat_transaction['Transaction Fee'] = transaction_fee

            balance_changes = transaction_record.get('balanceChanges', [])
            if balance_changes:

                amount = int(balance_changes[0].get('amount', '0'))  
                flat_transaction['Amount'] = amount


                if amount < 0:
                    net_transaction_value = amount + transaction_fee
                else:
                    net_transaction_value = amount - transaction_fee
                flat_transaction['Net Transaction Value'] = net_transaction_value
            flattened_data.append(flat_transaction)
    return flattened_data

def get_human_readable_timestamp(timestamp_ms):
    """
    This function converts a timestamp in milliseconds to a human-readable format.

    Parameters:
    timestamp_ms (int): The timestamp in milliseconds.

    Returns:
    str: A human-readable timestamp in the format 'dd-mm-yyyy'. If the input is None, an empty string is returned.
    """
    if timestamp_ms:
        timestamp_ms = int(timestamp_ms)  # Ensure timestamp_ms is an integer
        return pd.to_datetime(timestamp_ms, unit='ms').strftime('%d-%m-%Y')
    return ''

s3_client = boto3.client('s3')

def read_csv_from_s3(bucket, file_name):
    """Read a CSV file from S3 and return a DataFrame."""
    csv_obj = s3_client.get_object(Bucket=bucket, Key=file_name)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    return pd.read_csv(io.StringIO(csv_string))

def calculate_usd_values(merged_df):
    MIST_TO_SUI_RATIO = 1e9  


    merged_df['Amount_in_SUI'] = merged_df['Amount'] / MIST_TO_SUI_RATIO


    merged_df['amount_usd'] = merged_df['Amount_in_SUI'] * merged_df['SUI_USD']
    merged_df['amount_usd'] = merged_df['amount_usd'].apply(lambda x: "{:.6f}".format(x))

  
    merged_df['Net Transaction Value_in_SUI'] = merged_df['Net Transaction Value'] / MIST_TO_SUI_RATIO

 
    merged_df['net_transaction_in_USD'] = merged_df['Net Transaction Value_in_SUI'] * merged_df['SUI_USD']
    merged_df['net_transaction_in_USD'] = merged_df['net_transaction_in_USD'].apply(lambda x: "{:.6f}".format(x))

 
    merged_df['Reward_in_SUI'] = merged_df['Reward Amount'] / MIST_TO_SUI_RATIO
    merged_df['Reward_In_USD'] = merged_df['Reward_in_SUI'] * merged_df['SUI_USD']
    merged_df['Reward_In_USD'] = merged_df['Reward_In_USD'].apply(lambda x: "{:.6f}".format(x))

    return merged_df

def lambda_handler(event, context):
    
    """
    Main function to handle the AWS Lambda invocation.

    This function extracts the 'address' from the event object, queries transaction blocks and stakes for the given address,
    flattens the queried data and converts it into pandas DataFrames. It then filters the transactions DataFrame to exclude 
    'Staking' transactions with a 'Reward Amount' of 0. The stakes data is saved to a CSV file. The SUI-USD data is loaded 
    from a CSV file and merged with the transactions data. The merged data is processed to calculate the amount in USD and 
    net transaction value in USD. Unnecessary columns are dropped from the merged DataFrame. The processed data is saved to 
    a CSV file and also exported to an Excel file. Finally, the function returns a 200 status code with a success message.

    Parameters:
    event (dict): The event object containing the 'address'.
    context (obj): The AWS Lambda context object (not used in this function).

    Returns:
    dict: A dictionary containing the status code and body of the response.
    """

    address = event['queryStringParameters'].get('address') if event.get('queryStringParameters') else None
    if not address:
        return {"statusCode": 400, "body": json.dumps("Address parameter is missing!!!")}


    transactions = query_transaction_blocks(address)
    stakes = query_stakes(address)
    

    records_found = not (not transactions and not stakes)

    if not records_found:
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Record not found for the given address.", "records_found": False})
        }


    flattened_stakes = flatten_stake_data(stakes)
    stakes_df = pd.DataFrame(flattened_stakes)
    flattened_transactions = flatten_transaction_data(transactions, stakes_df, address)
    transactions_df = pd.DataFrame(flattened_transactions)


    transactions_df = transactions_df[(transactions_df['Transaction Type'] != 'Staking') | (transactions_df['Reward Amount'] != 0)]

    sui_usd_df = read_csv_from_s3(sui_usd_bucket_name, sui_usd_file_name)
    sui_usd_df['snapped_at'] = pd.to_datetime(sui_usd_df['snapped_at']).astype(int) // 10**6
    transactions_df['Unix Timestamp (ms)'] = transactions_df['Unix Timestamp (ms)'].astype(int)
    transactions_df.sort_values(by='Unix Timestamp (ms)', inplace=True)
    sui_usd_df.sort_values(by='snapped_at', inplace=True)
    
 
    merged_df = pd.merge_asof(transactions_df, sui_usd_df, left_on='Unix Timestamp (ms)', right_on='snapped_at', direction='nearest')
    merged_df.rename(columns={'price': 'SUI_USD'}, inplace=True)
    

    merged_df = calculate_usd_values(merged_df)
    
    merged_df.drop(columns=['snapped_at', 'market_cap', 'total_volume'], inplace=True)
   
    excel_filename = f"{address}_staked_unstaked_data.xlsx"  # Excel filename based on the address
    excel_buffer = io.BytesIO()
    merged_df.to_excel(excel_buffer, index=False, engine='openpyxl')
    excel_buffer.seek(0)
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=sui_usd_bucket_name, Key=excel_filename, Body=excel_buffer.getvalue())
    
   
    presigned_url = s3_client.generate_presigned_url('get_object',
                                                     Params={'Bucket': sui_usd_bucket_name, 'Key': excel_filename},
                                                     ExpiresIn=3600)  # Link expires in 1 hour
    
    # Return a response
    return {
        "statusCode": 200,
        "body": json.dumps({"message": presigned_url, "records_found": True})
    }