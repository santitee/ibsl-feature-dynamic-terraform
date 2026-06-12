import sys
import boto3
from awsglue.utils import getResolvedOptions
from datetime import datetime
from datetime import timedelta

glue_client = boto3.client('glue')


def get_workflow_params(workflow_name, workflow_run_id):
    return glue_client.get_workflow_run_properties(
        Name=workflow_name, RunId=workflow_run_id)['RunProperties']


def update_workflow_params(workflow_name, workflow_run_id,
                           old_params, new_params):
    glue_client.put_workflow_run_properties(
        Name=workflow_name, RunId=workflow_run_id,
        RunProperties={**old_params, **new_params})


def determine_workflow_params(workflow_name, workflow_position_date):
    system_date = datetime.now() + timedelta(hours=7)
    print(f'system_date : {system_date}')
    #frequency hourly it will be parse current date to position date and the first hour of midnight if will be parse previous date to position date.
    #check frequency hourly by parse workflow name to this condition
    #for the frequency hourly specific only source data that is UTC, There is not supported UTC+7
    print(f'workflow_name : {workflow_name}')
    if "hourly" in workflow_name :
        current_hour  = system_date.strftime('%H %p')
        cross_midnight_time = '07 AM'
        if current_hour != cross_midnight_time:
            position_date = datetime.now()
            # position_date = datetime.now() + timedelta(hours=7)
        else :
            # position_date = datetime.now() - timedelta(days=1) + timedelta(hours=7)
            position_date = datetime.now() - timedelta(days=1)
        print(f'current_hour : {current_hour}')
        print(f'cross_midnight_time : {cross_midnight_time}')
    #frequency daily regular it will be parse previous date to position date.
    else :
        if workflow_position_date:
            position_date = datetime.strptime(workflow_position_date, '%Y-%m-%d') 
        else:
            position_date = datetime.now() - timedelta(days=1) + timedelta(hours=7)

    print(f'position_date : {position_date}')

    workflow_params = dict()
    workflow_params['system_datetime'] = system_date.strftime(
        '%Y-%m-%d %H:%M:%S')
    workflow_params['utc_datetime'] = datetime.utcnow().strftime(
        '%a, %d %b %Y %H:%M:%S GMT')
    workflow_params['system_date'] = system_date.strftime('%Y-%m-%d')
    workflow_params['system_year'] = system_date.strftime('%Y')
    workflow_params['system_month'] = system_date.strftime('%m')
    workflow_params['system_day'] = system_date.strftime('%d')
    workflow_params['position_date'] = position_date.strftime('%Y-%m-%d')
    workflow_params['position_year'] = position_date.strftime('%Y')
    workflow_params['position_month'] = position_date.strftime('%m')
    workflow_params['position_day'] = position_date.strftime('%d')
    return workflow_params


args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])

workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']
workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
                                        RunId=workflow_run_id)["RunProperties"]
workflow_position_date = workflow_params['POSITION_DATE']
print(f'workflow_position_date : {workflow_position_date}')
update_workflow_params(workflow_name, workflow_run_id,
                       get_workflow_params(workflow_name, workflow_run_id),
                       determine_workflow_params(workflow_name,workflow_position_date))
## END OF SCRIPT
