from django.db import models

# Create your models here.
class ConfigItems(models.Model):
    key = models.TextField(unique=True)
    value = models.TextField()
    
    def __str__(self):
        return f"{self.key} : {self.value}"