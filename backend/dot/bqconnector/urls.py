from django.urls import path


from . import views 


urlpatterns = [
    path("", views.home_view , name="home-page"),
    path("connect",views.connect_view, name="bq-connect")
]