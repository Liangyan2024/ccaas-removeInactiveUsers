import json
import boto3
import csv
import os
from datetime import datetime

client = boto3.client('cognito-idp', region_name='ca-central-1')
user_pool_id = os.environ.get('Cognito_UserPool_ID')


connect_client = boto3.client('connect')
connect_instance_id = os.environ.get('Amazon_Connect_Instance_ID')
s3_bucket_name = os.environ.get('S3_Bucket')
exception_list = [item.strip() for item in os.environ.get('Exception_List').split(',')]


s3 = boto3.client('s3')


def lambda_handler(event, context):
    cognito_user_list = get_cognito_users(user_pool_id)
    
    
    connect_user_list = get_connect_users(connect_instance_id)
    
    delete_list1 = generate_delete_list1(cognito_user_list, connect_user_list)
    
    delete_list2 = generate_delete_list2(cognito_user_list, connect_user_list)
    
    delete_list3 = generate_delete_list3(cognito_user_list, connect_user_list)
    
    final_delete_list = {}
    for item in [delete_list1, delete_list2, delete_list3]:
        final_delete_list.update(item)

    for e in exception_list:
        final_delete_list.pop(e, None)

 
    if final_delete_list:   
        upload_logs_s3(s3_bucket_name, final_delete_list)


    return
    

def get_cognito_users(user_pool_id):
    
    cognito_user_list = {}
    next_token = None

    while True:
        
        params = {'UserPoolId': user_pool_id}
        if next_token:
            params['PaginationToken'] = next_token
        
        response = client.list_users(**params)

        for user in response['Users']:
            userInfo = {}
            userInfo = {'Username': user['Username']}
            userInfo ['date_created'] = (user['UserCreateDate']).strftime('%Y-%m-%d')
            userInfo ['Last_login']=  (user['UserLastModifiedDate']).strftime('%Y-%m-%d')
            user_id = None
            date_created = None

            attr_identity = next((attr for attr in user['Attributes'] if attr['Name'] == 'identities'), None)

            if attr_identity:
                try:
                    value_list = json.loads(attr_identity['Value'])
                    if value_list:
                        user_id = value_list[0].get('userId')
                except (json.JSONDecodeError, ValueError, TypeError) as e:
                    print(f"Error parsing 'identities': {e}")
            else:
                user_id = user['Username']
            if user_id:
                cognito_user_list[user_id] = userInfo

        next_token = response.get('PaginationToken')

        if not next_token:
            break

    return cognito_user_list   

def get_connect_users(connect_instance_id):
    
    security_profile_response = connect_client.list_security_profiles(InstanceId=connect_instance_id)
    hierarchy_group_response = connect_client.list_user_hierarchy_groups(InstanceId=connect_instance_id)
    connect_user_list = {}
    next_token = None

    while True:
        params = {'InstanceId': connect_instance_id}
        

        if next_token:
            params['NextToken'] = next_token

        user_response = connect_client.list_users(**params)

        for user in user_response['UserSummaryList']:
            user_id = user.get('Id')
            if user_id:
                try:
                    res_user = connect_client.describe_user (InstanceId=connect_instance_id, UserId=user['Id'])
                    user_data = res_user.get('User')
                    if user_data:
                        user_name = user_data.get('Username')
                        created = user_data.get('LastModifiedTime')
                        routing_profileId = user_data.get('RoutingProfileId')
                        identity_info = user_data.get('IdentityInfo')
                        security_profileIds = user_data.get('SecurityProfileIds')
                        hierarchy_group_Id = user_data.get('HierarchyGroupId')
                        Phone_Config = user_data.get('PhoneConfig') # get "AutoAccept"

                        if routing_profileId:
                            try:
                                res_routing_profile = connect_client.describe_routing_profile(InstanceId=connect_instance_id, RoutingProfileId=routing_profileId)
                                profile_name = res_routing_profile.get('RoutingProfile', {}).get('Name') or 'No_Routing_Profile'
                            except connect_client.exceptions.ResourceNotFoundException:
                                print(f"Routing profile {routing_profileId} not found for user {user_id}")
                            except Exception as e:
                                print(f"Failed to describe routing profile {routing_profileId}: {str(e)}")
                        else:
                            profile_name = 'N/A'

                        if identity_info:
                            first_name = identity_info.get('FirstName', 'No_Firstname')
                            last_name = identity_info.get('LastName', 'No_Lastname')
                        else:
                            first_name = 'N/A'
                            last_name = 'N/A'

                        if security_profileIds:
                            security_profile_name = []
                            for profileid in security_profileIds:
                                for p in security_profile_response['SecurityProfileSummaryList']:
                                    if profileid == p ['Id']:
                                        security_profile_name.append(p['Name'])
                        else:
                            security_profile_name = 'N/A'



                        if hierarchy_group_Id:
                            for h in hierarchy_group_response ['UserHierarchyGroupSummaryList']:
                                if h['Id'] == hierarchy_group_Id:
                                    hierarchy_group_name = h['Name']
                        else:
                            hierarchy_group_name = 'N/A'

                        if Phone_Config:
                            auto_accept = str(Phone_Config['AutoAccept']) 
                        else:
                            auto_accept ='N/A'
                
                        connect_user_list[user_name] = {
                            'first_name': first_name, 
                            'last_name': last_name, 
                            'date_created': created.strftime('%Y-%m-%d'), 
                            'RoutingProfile': profile_name, 
                            'SecurityProfile': ', '.join(security_profile_name), 
                            'Hierachy': hierarchy_group_name,
                            'AutoAccept': auto_accept,
                            'Hierachy': hierarchy_group_name}
                except connect_client.exceptions.ResourceNotFoundException:
                    print(f"User {user['Id']} not found.")
                except Exception as e:
                    print(f"Failed to describe user {user['Id']}: {str(e)}")
    
        next_token = user_response.get('NextToken')
        if not next_token:
            break 
    
    return connect_user_list
        

def generate_delete_list1(cognito_list, connect_list):
# Mark Amazon Connect users for deletion if not in Cognito and account is older than 6 months
    items2 = set(connect_list.keys())
    items1 = set(cognito_list.keys())
    only_in_connect = {}
    only_in_connect_key = items2 - items1
    
    for key in only_in_connect_key:
        if (datetime.today() - datetime.strptime(connect_list[key].get('date_created'), "%Y-%m-%d")).days > 180:
            only_in_connect[key] = connect_list[key]
            only_in_connect[key]['delete_reason'] = '1'
    

    return only_in_connect

def generate_delete_list2(cognito_list, connect_list):
# Mark Connect users with NHT profile as training accounts for deletion if inactive for 1+ month and account is 2+ months old
    nht_delete_list = {}
    for k,v in connect_list.items():
        if 'nht' in (v.get('RoutingProfile', '')).lower():
            if (datetime.today() - datetime.strptime(v.get('date_created'), "%Y-%m-%d")).days > 60:
                cognito_user = cognito_list.get(k)
                if cognito_user:
                    cognito_lastlogin = cognito_user.get('Last_login') 
                    if cognito_lastlogin:
                        if (datetime.today() - datetime.strptime(cognito_lastlogin, '%Y-%m-%d')).days > 30:
                            nht_delete_list[k] = v
                            nht_delete_list[k]['delete_reason'] = '2'

    return nht_delete_list

def generate_delete_list3(cognito_list, connect_list):
#Retrieves a dictionary of Amazon Connect users who have not logged into Cognito for the past 6 months (180 days). Only includes users who exist in both lists.
    cognito_inactive_list = {}
    for k, v in cognito_list.items():
        if (datetime.today() - datetime.strptime(v.get('Last_login'), "%Y-%m-%d")).days > 180:
            connect_user = connect_list.get(k)
            if connect_user:
                cognito_inactive_list[k] = connect_user
                cognito_inactive_list[k]['delete_reason'] = '3'

    return cognito_inactive_list


def upload_logs_s3(s3_bucket_name, delete_agent_list):
    bucket_name = s3_bucket_name
    csv_filename =  f'{datetime.today().date()}_deleted_agents.csv'
    s3_path = f'logs/{csv_filename}'

    csv_file_path = os.path.join('/tmp', csv_filename)
    
    
    with open(csv_file_path, mode='w', newline='') as file: 
        writer = csv.writer(file)
        writer.writerow([
            'LoginID', 'First Name', 'Last Name', 'Security Profile', 
            'Routing Profile', 'Hierarchy', 'AutoAccept', 'Delete Reason'
        ])

        for k, info in delete_agent_list.items():
            writer.writerow([
                k,
                info['first_name'],
                info['last_name'],
                info['SecurityProfile'],
                info['RoutingProfile'],
                info['Hierachy'],
                info['AutoAccept'],
                info['delete_reason']
            ])
    s3.upload_file(csv_file_path, bucket_name, s3_path)

    print(f"CSV uploaded to s3://{bucket_name}/{s3_path} successful")
        