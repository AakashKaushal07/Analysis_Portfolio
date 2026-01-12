from django.http import HttpResponse
from .services import EmailService
from base_app.models import ConfigItems
# Create your views here.

def test_mail(request):
    to_email = ConfigItems.objects.get(key="EMAIL_TO").value
    data = {
        "name" : "Aakash",
        "data" : [
            {"league" : "Liga MX 2025/2026 - Clausara", "game_count" : 374},
            {"league" : "Premier League 2025/2026", "game_count" : 200},
            {"league" : "1. SNL", "game_count" : 0}
        ]
    }
    EmailService.send_email("test_item",to_email,context_dict=data)
    
    return HttpResponse("Email Sent!")