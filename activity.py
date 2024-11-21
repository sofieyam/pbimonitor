import os
import json
import datetime
import time
import requests

def get_oauth_token(config):
    url = "https://login.microsoftonline.com/common/oauth2/token"

    payload = {
        'grant_type': 'password',
        'client_id': config['ServicePrincipal']['AppId'],
        'client_secret': config['ServicePrincipal']['AppSecret'],
        'username': 'dataplatform@gammonconstruction.com',
        'password': 'AW3$5RX6|ut1W9OjJ-xsMGd#rrG;Sy',
        'resource': 'https://analysis.windows.net/powerbi/api'
    }
        
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post(url, headers=headers, data=payload)
    response_data = response.json().get('access_token')

    return response_data

def get_activityevents(config, start_datetime, end_datetime): 
    access_token = get_oauth_token(config)
    
    continuation_url = None
    item_list = []
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    while True:
        if continuation_url:
            url = continuation_url
        else:
            url = f"https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime='{start_datetime}'&endDateTime='{end_datetime}'"
        
        response = requests.get(
            url,
            headers=headers
        )

        data = response.json()
        # print(data)
        continuation_url = data.get("continuationUri")
        item_list.extend(data.get("activityEventEntities") if data.get("activityEventEntities") else [])

        if not continuation_url:
            break
    
    return item_list

def fetch_activity_data(config, state_file_path):
    print("Starting Power BI Activity Fetch")
    start_time = time.time()

    output_batch_count = config.get('ActivityFileBatchSize', 5000)
    root_output_path = os.path.join(config['OutputPath'], 'activity')
    os.makedirs(root_output_path, exist_ok=True)

    # output_path = os.path.join(root_output_path, '{0:yyyy}', '{0:MM}')
    output_path = os.path.join(root_output_path)
    
    if not state_file_path:
        state_file_path = os.path.join(config['OutputPath'], 'state.json')

    if os.path.exists(state_file_path):
        with open(state_file_path, 'r') as file:
            state = json.load(file)
    else:
        state = {}

    max_history_date = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=29)).replace(hour=0, minute=0, second=0, microsecond=0)

    pivot_date = state.get('Activity', {}).get('LastRun')
    if pivot_date:
        pivot_date = datetime.datetime.fromisoformat(pivot_date).replace(tzinfo=datetime.timezone.utc)
    else:
        state['Activity'] = {'LastRun': None}
        pivot_date = max_history_date

    if pivot_date < max_history_date:
        print("Last run was more than 30 days ago")
        pivot_date = max_history_date

    print(f"Since: {pivot_date.strftime('%Y-%m-%dT%H:%M:%S')}")
    print(f"End: {(pivot_date + datetime.timedelta(hours=24) - datetime.timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S')}")
    print(f"OutputBatchCount: {output_batch_count}")


    # Print the value of pivot_date 

    while pivot_date <= datetime.datetime.now(datetime.timezone.utc):
        print(f"Getting audit data for: '{pivot_date.strftime('%Y%m%d')}'")

        start = pivot_date.strftime('%Y-%m-%dT%H:%M:%S')
        end = (pivot_date + datetime.timedelta(hours=24) - datetime.timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S')

        audits = get_activityevents(config, start, end)

        output_file_path = os.path.join(output_path, f"{pivot_date.strftime('%Y%m%d')}.json")   

        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        with open(output_file_path, 'w') as file:
            json.dump(audits, file)

        state['Activity']['LastRun'] = pivot_date.isoformat()
        pivot_date += datetime.timedelta(days=1)

        print("Saving state")
        os.makedirs(os.path.dirname(state_file_path), exist_ok=True)
        with open(state_file_path, 'w') as file:
            json.dump(state, file)

    elapsed_time = time.time() - start_time
    print(f"Elapsed: {elapsed_time}s")

try:
    # Load config from file
    with open('Config.json', 'r', encoding='utf-16') as file:
        config = json.load(file)
except Exception as e:
    print(e)

state_file_path = "./state.json"
fetch_activity_data(config, state_file_path)
