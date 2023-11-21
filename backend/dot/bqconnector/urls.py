from django.urls import path


from . import views 


urlpatterns = [
    path("", views.upload_csv , name="home-page"),
    # path("connect",views.connect_view, name="bq-connect"),
    path("upload",views.upload_csv,name="upload-csv-to-bq"),
    path("ingest",views.get_table_data,name="get-table-data"),
    path("ingest/form",views.ingest_form,name="ingest-form-user-data"),
    path("dataplex/job/status",views.dataplex_job_status,name="dataplex-job-status")
]