from django.db import models

# Create your models here.
class ConfigItems(models.Model):
    key = models.TextField(unique=True)
    value = models.TextField()
    
    def __str__(self):
        return f"{self.key} : {self.value}"

class OptaEvents(models.Model):
    opta_id = models.TextField(unique=True)
    event_name = models.TextField()
    description = models.TextField()
    
    def __str__(self):
        return f"{self.event_name} : {self.opta_id}"

class OptaQualifier(models.Model):
    opta_id = models.TextField(unique=True)
    qualifier_name = models.TextField()
    description = models.TextField()
    
    def __str__(self):
        return f"{self.qualifier_name} : {self.opta_id}"
