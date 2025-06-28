from django.db import models
from leagues.models import Season
class Season(models.Model):
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['season', 'home_team', 'away_team'],
                name='unique_season_key'
            )
        ]
    season = models.ForeignKey(Season, on_delete=models.CASCADE)
    home_team = models.CharField(max_length=50, null=False, blank=False)
    away_team = models.CharField(max_length=60, null=False, blank=False)
    match_date = models.DateTimeField(null=False, blank=False)
    EVENT_STATUS_CHOICES = [
        ('not_done', 'Not Done'),
        ('in_progress', 'In Progress'),
        ('completed', 'Completed'),
        ('error', 'Error'),
    ]
    event_status = models.CharField(
        max_length=20,
        choices=EVENT_STATUS_CHOICES,
        default='not_done',
        null=False,
        blank=False
    )
    
    def __str__(self):
        return f"{self.competition.competition_name} - {self.name} - FM({self.name_fotmob})"
