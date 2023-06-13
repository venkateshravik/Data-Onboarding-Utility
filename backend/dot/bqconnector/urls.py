from django.urls import path


from . import views 


urlpatterns = [
    path("", views.home_view , name="home-page"),
    path("connect",views.connect_view, name="bq-connect"),
    path("upload",views.upload_csv,name="upload-csv-to-bq")
]