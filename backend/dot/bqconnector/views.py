from django.shortcuts import render
from django.core.exceptions import ObjectDoesNotExist
from django.http import JsonResponse, HttpResponse , HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from google.cloud import bigquery
from django.conf import settings
from django.urls import reverse
from bqconnector.models import JobIdStore , BigqueryInfo 
import os
import csv
import json
import threading

#dq scan 
from google.cloud import dataplex_v1
from google.cloud.dataplex_v1.types import DataQualityRule
from google.api_core.exceptions import NotFound
from time import sleep
import re
####

# TABLE_ID = "gcpsubhrajyoti-test-project.dot_testing.dot_result"

# SOURCE_TABLE_NAME = "dot_result"
# RESULT_TABLE_NAME = "dot-target-table"
# DATASET = "dot_testing"
PROJECT_ID = "gcpsubhrajyoti-test-project"
LOCATION = "us-central1"
JOB_NAME = ""

# Create your views here.
@login_required
def home_view(request):
    context = {}
    return render(request,'home.html',context)

# create a separate dataset for every upload
def create_bigquery_dataset(dataset_name):
    dataset_id = PROJECT_ID + '.' + dataset_name
    client = bigquery.Client()
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    client.create_dataset(dataset)


#create 3 tables stg , source , target
def create_bigquery_tables(table_id, dataset_id):
    client = bigquery.Client(project= PROJECT_ID)


    # Create an empty table reference.
    table_ref = client.dataset(dataset_id).table(table_id)

    # Create the empty table.
    table = bigquery.Table(table_ref)
    return client.create_table(table)


def add_primary_key(SOURCE_DATASET_NAME, SOURCE_TABLE_NAME):
    client = bigquery.Client(project = PROJECT_ID)
    SOURCE_TABLE_REF = PROJECT_ID + "." + SOURCE_DATASET_NAME + "." + SOURCE_TABLE_NAME
    query_to_add_PK = """
    SELECT
    ROW_NUMBER() OVER () AS id,
    A.*
    FROM
    `"""+SOURCE_TABLE_REF+"""` A ; """

    job_config = bigquery.QueryJobConfig(destination=SOURCE_TABLE_REF, write_disposition = "WRITE_TRUNCATE")
    query_job = client.query(query_to_add_PK, job_config=job_config)

    # Wait for the query to finish.
    query_job.result()
    return


@login_required
def upload_csv(request):

    # Check if the form has been submitted
    if request.method == "POST":
        file = request.FILES['csv_file']
        file_name = (file.name).split('.')[0]
        file_path = os.path.join(settings.MEDIA_ROOT, file.name)
        user = request.user

        with open(file_path, "wb") as f:
            f.write(file.file.read())

        client = bigquery.Client()

        #creating new dataset
        try:
            file_name = re.sub("[^A-Za-z0-9]", "", file_name)
            dataset_id = file_name + '_dst'
            create_bigquery_dataset(dataset_id)

            #creating new tables
            source_table_id = file_name[:20] + '_src'
            target_table_id = file_name[:20] + '_tgt'
            temp_source_tbl = create_bigquery_tables(source_table_id,dataset_id)
            temp_result_tbl = create_bigquery_tables(target_table_id, dataset_id)
            # bigquery_file_name, source_table_name , target_table_name , dataset_id ,
            TABLE_ID = temp_source_tbl

            #create the database object 
            BigqueryInfo.objects.create(bigquery_file_name=file_name, 
            dataset_name=dataset_id,source_table_id=temp_source_tbl,target_table_id=temp_result_tbl,user=user)



            table_id = TABLE_ID
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
            )

            with open(file_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_id, job_config=job_config)

            job.result()  # Waits for the job to complete.
            table = client.get_table(table_id)  # Make an API request.
            
            msg = "Loaded {} rows and {} columns from file to {}. \nAdded row number for validation transparency".format(
                table.num_rows, len(table.schema), table_id
            )
        
            #Add primary key in place to the table
            add_primary_key(dataset_id, source_table_id)
        except Exception as e:
            return render(request, "upload.html", {"error": True,"message":str(e)[:500]})
        return HttpResponseRedirect('/ingest')
    else:
        return render(request, "upload.html")


############################################################################################
#dq jobs
############################################################################################
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
            # print('Incorrect Rule type')
            continue
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
    # print("Waiting for data scan object to be created...")
    data_scan_obj = operation.result()
    # print(data_scan_obj)
    return data_scan_obj


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
    except NotFound as e:
        data_scan_obj = create_data_scan(dataplex_client, data_scan_unique_name, rules_config, project_id, source_table, results_table, locations)
    data_scan_name = data_scan_obj.name
    job_response = dataplex_client.run_data_scan(name=data_scan_name)
    return job_response.job.name


def create_valid_rows_table(request,DQ_JOB_ID, DATAPLEX_JOB_METADATA_TABLE_ID):
    user = request.user
    bq_client = bigquery.Client()
    bigqueryinfo_obj = BigqueryInfo.objects.filter(user=user).last()
    DATASET = bigqueryinfo_obj.dataset_name
    SOURCE_TABLE_NAME = bigqueryinfo_obj.source_table_id
    DESTINATION_TABLE_NAME = bigqueryinfo_obj.target_table_id

    DQ_JOB_METADATA_TABLE_REF = PROJECT_ID + "." + DATASET + "." + DATAPLEX_JOB_METADATA_TABLE_ID

    # Query a BigQuery table.
    query = """
    SELECT rule_failed_records_query FROM `"""+DQ_JOB_METADATA_TABLE_REF+"""` WHERE data_quality_job_id = '"""+DQ_JOB_ID+"""'
    and rule_passed = false
    """
    # print(query)
    # Run the query.
    query_job = bq_client.query(query)

    # Wait for the query to finish.
    result = query_job.result()

    union_all_query_string=""

    if result.total_rows > 0:
        for row in result:
            if union_all_query_string=="":
                union_all_query_string = str(row[0]).replace(';','')
            else:
                union_all_query_string += "\n UNION ALL \n" + str(row[0]).replace(';','')

    # query_for_valid_rows = """select * EXCEPT(id) from `"""+SOURCE_TABLE_NAME+"""` where id not in
    # (select distinct(temp.id) from ("""+ union_all_query_string + """) temp) """
    if union_all_query_string.strip():
        remove_invalid_rows_query = """ where id not in
    (select distinct(temp.id) from ("""+ union_all_query_string + """) temp) """
    else:
        remove_invalid_rows_query = ""
    query_for_valid_rows = """select * EXCEPT(id) from `"""+SOURCE_TABLE_NAME+"""`""" + remove_invalid_rows_query
    # print(query_for_valid_rows)

    # Set the destination for the results.
    job_config = bigquery.QueryJobConfig(destination=DESTINATION_TABLE_NAME, write_disposition = "WRITE_TRUNCATE")

    query_job = bq_client.query(query_for_valid_rows, job_config=job_config)

    # Wait for the query to finish.
    query_job.result()


def trigger_final_table_insertion(request,data_scan_name: str, dataplex_job_metadata_table: str):
    client = dataplex_v1.DataScanServiceClient()
    status = "PENDING"
    # Get the data scan job object.
    while status.upper() in ("PENDING","RUNNING"):
        job = client.get_data_scan_job(
            name=data_scan_name
        )
        # print(job.state)
        sleep(5)
        status = str(job.state)[6:]
    job_id = data_scan_name.split("/")[-1]
    create_valid_rows_table(request,job_id, dataplex_job_metadata_table)   
    return status

#############################################################

def get_table_data(request):
    try:
        user = request.user
        client = bigquery.Client()
        bigquery_info_obj = BigqueryInfo.objects.filter(user=user).last()
        table_id = bigquery_info_obj.source_table_id
        table = client.get_table(table_id)
        schema = table.schema
        table_len = len(table.schema)
    except Exception as e:
        return render(request, "ingest.html", {"error": True,"message":"Missing table info, Please upload a csv file"})
    return render(request, "ingest.html", {"success": True,"table":schema,"table_len":range(table_len)})


def get_col_name_for_ingest_form(request):
    user = request.user
    client = bigquery.Client()
    bigqueryinfo_obj = BigqueryInfo.objects.filter(user=user).last()
    table_id = bigqueryinfo_obj.source_table_id
    table = client.get_table(table_id)
    column_names = []
    for schema_field in table.schema:
        column_names.append(schema_field.name)
    return column_names 


def get_rules_config(rules_config_list,col_name):
    rules = []
    dimension = [ "VALIDITY", "COMPLETENESS","VALIDITY","VALIDITY","UNIQUENESS", "VALIDITY"]
    for i in range(len(col_name)):
        column = col_name[i]
        check_obj = rules_config_list[str(i)]
        temp_rules_config_list = {}
        temp_rules_config_list['column']=column
        if "range_expectation" in check_obj :
            min_value = check_obj['range_expectation'][0]
            max_value = check_obj['range_expectation'][1]
            ignore_null = check_obj['range_expectation'][2]
            strict_min_enabled = check_obj['range_expectation'][3]
            strict_max_enabled = check_obj['range_expectation'][4]
            temp_rules_config_list['dq_check_name'] = "range_expectation"
            temp_rules_config_list['dq_check_properties'] = {"min_value":min_value,"max_value":max_value,"strict_min_enabled":strict_min_enabled,"strict_max_enabled":strict_max_enabled}
            temp_rules_config_list['dimension'] = dimension[0]

        elif 'non_null_expectation' in check_obj:
            default_value = check_obj['non_null_expectation'][0]
            temp_rules_config_list['dq_check_name'] = 'non_null_expectation'
            temp_rules_config_list['dq_check_properties'] = {"default_value":default_value}
            temp_rules_config_list['dimension'] = dimension[1]

        elif 'set_expectation' in check_obj:
            ignore_null = check_obj['set_expectation'][0]
            values = check_obj['set_expectation'][1]
            temp_rules_config_list['dq_check_name']='set_expectation'
            temp_rules_config_list['dq_check_properties'] = {"ignore_null":ignore_null,"values":values}
            temp_rules_config_list['dimension']=dimension[2]

        elif 'regex_expectation' in check_obj:#raise error if it's not a regular expression
            regex = check_obj['regex_expectation'][0]
            temp_rules_config_list['dq_check_name']='regex_expectation'
            temp_rules_config_list['dq_check_properties']={"regex":regex}
            temp_rules_config_list['dimension']=dimension[3]

        elif "uniqueness_expectation" in check_obj:
           ignore_null = check_obj["uniqueness_expectation"][0]
           temp_rules_config_list['dq_check_name']="uniqueness_expectation"
           temp_rules_config_list['dq_check_properties']={"ignore_null":ignore_null}
           temp_rules_config_list['dimension']=dimension[4]

        elif 'statistic_range_expectation' in check_obj:
            min_value = check_obj['statistic_range_expectation'][0]
            max_value = check_obj['statistic_range_expectation'][1]
            strict_min_enabled = check_obj['strict_min_enabled'][2]
            strict_max_enabled = check_obj['strict_max_enabled'][3]
            temp_rules_config_list['dq_check_name']='statistic_range_expectation'
            temp_rules_config_list['dq_check_properties']={'min_value':min_value,'max_value':max_value,'strict_min_enabled':strict_min_enabled,'strict_max_enabled':strict_max_enabled}
            temp_rules_config_list['dimension']=dimension[5]
        else :
            temp_rules_config_list['dimension'] = None
            temp_rules_config_list['dq_check_name']=None
            temp_rules_config_list['dq_check_properties']=None
        rules.append(temp_rules_config_list)
    return rules

'''
modfied the data and feed into the source table
'''
#TODO
def update_default_value_and_description(request,description_list,default_list):
    user = request.user
    client = bigquery.Client()
    Bigqueryinfo_obj = BigqueryInfo.objects.filter(user=user).last()
    table_id = Bigqueryinfo_obj.source_table_id
    table = client.get_table(table_id)
    column_names = []
    count = 0
    for schema_field in table.schema:
        # print(schema_field)
        schema_field.description = description_list[count]
        schema_field.default_value = default_list[count]
        count = count + 1
    pass


@login_required
def ingest_form(request):
    jsonStr = request.body.decode("utf-8")
    result = json.loads(jsonStr)
    #print(result)
    description_list = result['describeInput']
    default_list = result['defaultInput']
    # update_default_value_and_description(request,description_list,default_list)
    rules_config_list = result['ruleTypeInput']
    gcp_input_list = result['gcpInput']
    project_id = gcp_input_list['gcpProjectId']
    col_name = get_col_name_for_ingest_form(request)
    rules = get_rules_config(rules_config_list,col_name)

    try:
        #put the parameters in the execute block 
        user = request.user
        bigqueryinfo_obj = BigqueryInfo.objects.filter(user=user).last()
        temp_source_tbl = bigqueryinfo_obj.source_table_id or ""
        temp_result_tbl = bigqueryinfo_obj.target_table_id or ""
        SOURCE_TABLE_NAME = temp_source_tbl.split('.')[2]
        RESULT_TABLE_NAME = temp_result_tbl.split('.')[2]
        DATASET = bigqueryinfo_obj.dataset_name 
        DATAPLEX_JOB_METADATA_TABLE_ID= "dataplex_job_metadata"
        # print(DATASET)
        JOB_NAME = execute(rules, PROJECT_ID, DATASET, SOURCE_TABLE_NAME, DATAPLEX_JOB_METADATA_TABLE_ID,LOCATION)
        thread = threading.Thread(target=trigger_final_table_insertion, args=(request,JOB_NAME, DATAPLEX_JOB_METADATA_TABLE_ID, ))
        thread.start()
        JobIdStore.objects.create(name = JOB_NAME,user=user)
        return JsonResponse({'msg':'success','job_name':JOB_NAME})
    except Exception as e:
        # print(e)
        return JsonResponse({'error':str(e),'status':400})
    
@login_required
def dataplex_job_status(request):
    client = dataplex_v1.DataScanServiceClient()
    status = "PENDING"
    try:
        user = request.user
        JobIdStore_obj = JobIdStore.objects.filter(user=user).last()
        temp = JobIdStore_obj.name or ""
        job = client.get_data_scan_job(
            name=  temp
        )
        # JobIdStore.objects.create_or_(name=job.name,user=user,defaults={"name" : job.name,"job_start_time":job.start_time,"job_end_time":job.end_time,"job_status":job.state})
        # job_list = JobIdStore.objects.filter(user=user).all()[:5]
        # print(job_list)
        stats = {
            "scan_job_name": job.name,
            "start_time":job.start_time,
            "end_time":job.end_time,
            "state":job.state,
            "res":str(job)[-37:],
        
        }
        
    except Exception as e :
        return render(request,"dataplexJobStatus.html",{"message":"No Jobs Found"})
    return render(request,"dataplexJobStatus.html",{"status":stats})