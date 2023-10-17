from django.db import models

# Create your models here.




class JobIdStore(models.Model):
    name = models.CharField(max_length=300)
    created_date = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name