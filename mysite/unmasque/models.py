# Create your models here.
from django.db import models


class QueryResult(models.Model):
    result = models.CharField(max_length=255)
