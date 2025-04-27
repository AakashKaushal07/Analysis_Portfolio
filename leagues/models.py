from django.db import models
class Competition(models.Model):
    CONFEDERATION_CHOICES = [
        ('AFC', 'AFC'), # (Stored in DB, Displayed in Admin)
        ('CAF', 'CAF'),
        ('CONCACAF', 'CONCACAF'),
        ('CONMEBOL', 'CONMEBOL'),
        ('OFC', 'OFC'),
        ('UEFA', 'UEFA'),
        ('FIFA', 'FIFA'),
    ]   
    MONTHS_CHOICES = [
        (1, 'January'),
        (2, 'February'),
        (3, 'March'),
        (4, 'April'),
        (5, 'May'),
        (6, 'June'),
        (7, 'July'),
        (8, 'August'),
        (9, 'September'),
        (10, 'October'),
        (11, 'November'),
        (12, 'December'),
    ]
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['confederation', 'country', 'competition_name'],
                name='unique_competition_key'
            )
        ]
    # geographical information
    confederation = models.CharField(max_length=10, choices=CONFEDERATION_CHOICES)
    country = models.CharField(max_length=25, null=False, blank=False)
    
    # competition information
    competition_name = models.CharField(max_length=30, null=False, blank=False)
    competition_format = models.CharField(max_length=1, choices=[('L', 'League'), ('C', 'Cup'), ("H", "Hybrid")])
    competition_type = models.CharField(max_length=1, choices=[('D', 'Domestic'), ('I', 'International')])
    season_start = models.IntegerField(choices=MONTHS_CHOICES)
    season_end = models.IntegerField(choices=MONTHS_CHOICES)
    
    # data collection information
    event_data_available = models.BooleanField(max_length=1, default=True, choices=[(True, 'Yes'), (False, 'No')])
    shot_data_available = models.BooleanField(max_length=1, default=False, choices=[(True, 'No'), (False, 'Yes')])

    def __str__(self):
        return f"{self.competition_name} - {self.country}"

class Season(models.Model):
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['competition', 'year'],
                name='unique_season_key'
            )
        ]
    competition = models.ForeignKey(Competition, on_delete=models.CASCADE)
    name = models.CharField(max_length=25, null=False, blank=False)
    
    # competition information
    season_event_url = models.URLField(null=True,blank=True,default="")
    season_shot_url = models.URLField(null=True,blank=True,default="")
    
    def __str__(self):
        return f"{self.competition.competition_name} - {self.names}"
