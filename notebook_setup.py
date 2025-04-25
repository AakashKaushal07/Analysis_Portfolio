import django
import os
os.chdir('D:/Analysis_Portfolio')
# print(f"Script is being triggered from: {os.getcwd()}")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analysis_portfolio.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

django.setup()
