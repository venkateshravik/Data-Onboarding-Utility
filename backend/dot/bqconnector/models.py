from django.db import models

# Create your models here.

class User(models.Model):
    first_name = models.CharField(max_length=100)
    last_name = models.CharField(max_length=30)
    created = models.DateTimeField(auto_now=True)
    email = models.EmailField(unique=True)
    password = models.CharField(max_length=30)

    def __str__(self):
        return self.email



class JobIdStore(models.Model):
    name = models.CharField(max_length=300)
    created_date = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name