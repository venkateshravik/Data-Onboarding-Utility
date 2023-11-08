from django.db import models

# Create your models here.




class JobIdStore(models.Model):
    name = models.CharField(max_length=300)
    created_date = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

class BigqueryInfo(models.Model):
    bigquery_file_name = models.CharField(max_length=1024)
    dataset_name = models.CharField(max_length=1024)
    source_table_id = models.CharField(max_length=1024)
    target_table_id = models.CharField(max_length=1024)

    def __str__(self):
        return self.bigquery_file_name