from django.db import models
from django.utils import timezone
class EmailTemplate(models.Model):
    """
    Stores the design of your emails. 
    Allows you to change text without touching code.
    """
    slug = models.SlugField(unique=True, help_text="Unique key to identify this template (e.g. 'task_reminder')")
    subject_template = models.CharField(max_length=255, help_text="Supports {{ variables }}")
    body_template = models.TextField(help_text="HTML content. Supports {{ variables }}")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.slug

class EmailLog(models.Model):
    """
    The history of every email ever attempted.
    """
    STATUS_CHOICES = (
        ('QUEUED', 'Queued'),
        ('SENT', 'Sent'),
        ('FAILED', 'Failed'),
    )

    template = models.ForeignKey(EmailTemplate, on_delete=models.SET_NULL, null=True)
    recipient = models.EmailField()
    subject = models.CharField(max_length=255)  # The final subject after rendering
    body = models.TextField()  # The final HTML after rendering
    
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='QUEUED')
    error_message = models.TextField(blank=True, null=True) # If Gmail complains, store why
    
    context_data = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    sent_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.subject} - {self.status} | To : {self.created_at}"

# Create your models here.
