from django.template import Template, Context
from django.core.mail import send_mail, get_connection
from django.utils import timezone
import os
from .models import EmailTemplate, EmailLog
# Replace 'my_core_app' with the actual app name where you store SystemConfig
from base_app.models import ConfigItems 

class EmailService:
    @staticmethod
    def send_email(template_slug, recipient_email, context_dict=None):
        if context_dict is None:
            context_dict = {}

        # 1. Fetch Template
        try:
            template_obj = EmailTemplate.objects.get(slug=template_slug)
        except EmailTemplate.DoesNotExist:
            print(f"Error: Template {template_slug} not found!")
            return None

        # 2. Render Content
        subject = Template(template_obj.subject_template).render(Context(context_dict))
        body = Template(template_obj.body_template).render(Context(context_dict))

        # 3. Create Log (Status: IN_PROGRESS immediately)
        email_log = EmailLog.objects.create(
            template=template_obj,
            recipient=recipient_email,
            subject=subject,
            body=body,
            context_data=context_dict,
            status='IN_PROGRESS'
        )

        try:
            # 4. Fetch & Decrypt Credentials
            mail_user = ConfigItems.objects.get(key="EMAIL_USER").value
            real_password = os.environ['APP_PASS']
            hostname = ConfigItems.objects.get(key="EMAIL_HOST").value
            port = ConfigItems.objects.get(key="EMAIL_PORT").value
            use_tls = ConfigItems.objects.get(key="EMAIL_USE_TLS").value
            # 5. Open Connection
            connection = get_connection(
                host=hostname,
                port=port,
                username=mail_user,
                password=real_password,
                use_tls=use_tls
            )

            # 6. Send
            send_mail(
                subject=subject,
                message=body, # Plain text fallback
                html_message=body,
                from_email=mail_user,
                recipient_list=[recipient_email],
                connection=connection,
                fail_silently=False,
            )

            # 7. Success Update
            email_log.status = 'SENT'
            email_log.sent_at = timezone.now()

        except Exception as e:
            # 8. Failure Update
            email_log.status = 'FAILED'
            email_log.error_message = str(e)
            # Re-raise error if you want the user to see the crash page, 
            # otherwise just print it and let the user continue.
            print(f"Email Failed: {e}")

        finally:
            email_log.save()

        return email_log