from .models import ConfigItems

def fetch_configurations():
        config_dict = {}
        for item in ConfigItems.objects.all():
            config_dict[item.key] = item.value
        return config_dict