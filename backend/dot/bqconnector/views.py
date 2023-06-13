from django.shortcuts import render
from django.http import JsonResponse, HttpResponse , HttpResponseRedirect
from google.cloud import bigquery
from django.conf import settings
import os
import csv

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


def upload_csv(request):

    # Check if the form has been submitted
    if request.method == "POST":
        file = request.FILES['csv_file']
        file_path = os.path.join(settings.MEDIA_ROOT, file.name)

        with open(file_path, "wb") as f:
            f.write(file.file.read())

        # # Read the file and process it
        # csv_data = csv.reader(open(file_path, "r"))
        # for row in csv_data:
        #     # Do something with the data
        #     print(row)
        
        # Construct a BigQuery client object.
        client = bigquery.Client()
        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = "gcpsubhrajyoti-test-project.dot_testing.dot_result"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
        )

        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        job.result()  # Waits for the job to complete.
        table = client.get_table(table_id)  # Make an API request.
        
        msg = "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        return render(request, "upload.html", {"success": True,"message":msg})
    else:
        return render(request, "upload.html")
