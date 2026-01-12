from django.contrib import admin
from .models import EmailTemplate, EmailLog

@admin.register(EmailTemplate)
class EmailTemplateAdmin(admin.ModelAdmin):
    list_display = ('slug', 'created_at')

@admin.register(EmailLog)
class EmailLogAdmin(admin.ModelAdmin):
    list_display = ('recipient', 'template', 'status', 'created_at', 'sent_at')
    list_filter = ('status', 'template')
    readonly_fields = ('subject', 'body', 'error_message', 'context_data')
# Register your models here.
