from django.db import models
from django.contrib.auth.models import User
# Create your models here.




class JobIdStore(models.Model):
    name = models.CharField(max_length=300)
    created_date = models.DateTimeField(auto_now=True)
    user         = models.ForeignKey(User,on_delete=models.CASCADE)

    def __str__(self):
        return self.name

class BigqueryInfo(models.Model):
    bigquery_file_name = models.CharField(max_length=1024)
    dataset_name = models.CharField(max_length=1024)
    source_table_id = models.CharField(max_length=1024)
    target_table_id = models.CharField(max_length=1024)
    user            = models.ForeignKey(User,on_delete=models.CASCADE)

    def __str__(self):
        return self.bigquery_file_name
