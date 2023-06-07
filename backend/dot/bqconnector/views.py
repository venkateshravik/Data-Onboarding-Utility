from django.shortcuts import render
from django.http import JsonResponse, HttpResponse , HttpResponseRedirect
from google.cloud import bigquery

# Create your views here.

def home_view(request):
    context = {}
    return render(request,'home.html',context)

# use the default google cloud login credentials for authentication
#



def connect_view(request):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = "data-onboarding-utility.your_dataset.your_table_name" #need to create the table id

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("post_abbr", "STRING"),
            bigquery.SchemaField("date", "DATE"),
        ],
        skip_leading_rows=1,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",  # Name of the column to use for partitioning.
            expiration_ms=7776000000,  # 90 days.
        ),
    )
    uri = "gs://cloud-samples-data/bigquery/us-states/us-states-by-date.csv"

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)   

    load_job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)
    print("Loaded {} rows to table {}".format(table.num_rows, table_id))
    context = {}
    return render(request,"connect_view.html",context)
