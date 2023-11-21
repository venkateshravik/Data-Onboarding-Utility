
from google.cloud import dataplex_v1
from google.cloud.dataplex_v1.types import DataQualityRule
from google.api_core.exceptions import NotFound
from time import sleep
import re


def create_data_scan(client: dataplex_v1.DataScanServiceClient, 
                     data_scan_name: str, 
                     rules_config: list, 
                     project_id: str,
                     source_table: str, 
                     results_table: str,
                     locations: str):
    data_scan = dataplex_v1.DataScan()
    data_scan.data_quality_spec.rules=[]
    for rule in rules_config:
        dq_rule = DataQualityRule()
        dq_rule.column = rule['column']
        dq_rule.dimension = rule['dimension']
        if rule['dq_check_name'] == "range_expectation":
            dq_rule.range_expectation = DataQualityRule.RangeExpectation()
            if 'min_value' in rule['dq_check_properties']:
                dq_rule.range_expectation.min_value = rule['dq_check_properties']['min_value']
            if 'max_value' in rule['dq_check_properties']:
                dq_rule.range_expectation.max_value = rule['dq_check_properties']['max_value']
                #TODO add strict min max
        elif rule['dq_check_name'] == "non_null_expectation":
            dq_rule.non_null_expectation = DataQualityRule.NonNullExpectation()
        elif rule['dq_check_name'] == "set_expectation":
            dq_rule.set_expectation = DataQualityRule.SetExpectation()
            dq_rule.set_expectations.values =  rule['dq_check_properties']['values']
        elif rule['dq_check_name'] == "regex_expectation":
            dq_rule.regex_expectation = DataQualityRule.RegexExpectation()
            dq_rule.set_expectations.regex =  rule['dq_check_properties']['regex']
        elif rule['dq_check_name'] == "uniqueness_expectation":
            dq_rule.uniqueness_expectation = DataQualityRule.UniquenessExpectation()
        elif rule['dq_check_name'] == "statistic_range_expectation":
            dq_rule.statistic_range_expectation = DataQualityRule.StatisticRangeExpectation()
            if 'statistic' in rule['dq_check_properties']:
                dq_rule.range_expectation.statistic = dq_rule.statistic_range_expectation.ColumnStatistic(rule['dq_check_properties']['statistic'])
            if 'min_value' in rule['dq_check_properties']:
                dq_rule.range_expectation.min_value = rule['dq_check_properties']['min_value']
            if 'max_value' in rule['dq_check_properties']:
                dq_rule.range_expectation.max_value = rule['dq_check_properties']['max_value'] 
        else:
            print('Incorrect Rule type')
        data_scan.data_quality_spec.rules.append(dq_rule)
    
    # data_scan.data_quality_spec.post_scan_actions = PostScanActions()
    # data_scan.data_quality_spec.post_scan_actions.bigquery_export = PostScanActions.BigQueryExport()
    data_scan.data_quality_spec.post_scan_actions.bigquery_export.results_table = results_table

    data_scan.data.resource = source_table
    
    request = dataplex_v1.CreateDataScanRequest(
        parent=f"projects/{project_id}/locations/{locations}",
        data_scan=data_scan,
        data_scan_id=data_scan_name,
    )

    # Make the request
    operation = client.create_data_scan(request=request)
    print("Waiting for data scan object to be created...")
    data_scan_obj = operation.result()
    print(data_scan_obj)
    return data_scan_obj


def poll_job_status_UI(data_scan_name: str):
    client = dataplex_v1.DataScanServiceClient()
    status = "PENDING"
    # Get the data scan job object.
    while status.upper() in ("PENDING","RUNNING"):
        job = client.get_data_scan_job(
            name=data_scan_name
        )
        print(job.state)
        #TO-DO print this result to UI @SJ
        sleep(5)
        status = str(job.state)[6:]   
    return status


def get_data_scan_unique_name(table_name):
    regex = re.compile(r"[^a-zA-Z0-9-]")
    table_name_wo_spcl_char = regex.sub("-", table_name)
    return f"dq-check-{table_name_wo_spcl_char.lower()}"


def execute(rules_config: list, project_id: str, dataset: str, source_table_name: str, results_table_name: str, locations:str = "us-central1"):
    dataplex_client = dataplex_v1.DataScanServiceClient()
    source_table = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset}/tables/{source_table_name}"
    results_table =f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset}/tables/{results_table_name}"
    data_scan_unique_name = get_data_scan_unique_name(source_table_name)
    try:
        data_scan_obj = dataplex_client.get_data_scan(
        name=f"projects/{project_id}/locations/{locations}/dataScans/{data_scan_unique_name}"
        )
        print(f"Data scan object exists already as {data_scan_unique_name}")
        # TO-DO update dq check config when rules are changed by the user for the source table
        # data_scan_rules = str(data_scan_obj.data_profile_spec)
        # print(f"data_scan_rules: {data_scan_rules}")
        # input_rules = str(rules_config)
        # print(f"input_rules: {input_rules}")
        # if data_scan_rules!= input_rules:
        #     update_data_scan(dataplex_client, data_scan_unique_name, rules_config)
    except NotFound as e:
        print(e)
        print("Not found: creating now")
        data_scan_obj = create_data_scan(dataplex_client, data_scan_unique_name, rules_config, project_id, source_table, results_table, locations)
    data_scan_name = data_scan_obj.name
    print(f"Triggering data scan run for the object {data_scan_unique_name}")
    job_response = dataplex_client.run_data_scan(name=data_scan_name)
    print(f"job creation resp: {job_response}")
    return job_response.job.name


if __name__=="__main__":
    rules_config = [ {
            "column": "d_app_name",
            "dimension": "UNIQUENESS",
            "dq_check_name":"uniqueness_expectation",                
            "dq_check_properties": {}
            }]
    job = execute(rules_config, 'pso-rn-playground', 'user_config', 'user_config','dq_result_user_config')
    print(poll_job_status_UI(job))
