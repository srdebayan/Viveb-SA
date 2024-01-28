# models.py

from django.db import models

class VivebFile(models.Model):
    FILE_TYPES = (
        ('saga', 'Saga File'),
        ('saga_instance', 'Saga Instance File'),
        ('saga_log', 'Saga Log File'),
    )

    file = models.FileField(upload_to='viveb_files/')  # 'saga_files/' is the subdirectory in MEDIA_ROOT
    file_type = models.CharField(max_length=20, choices=FILE_TYPES)

    def __str__(self):
        return self.file.name
    
    def base_filename(self):
        print(self.file.name.split('/')[-1],flush=True)
        return self.file.name.split('/')[-1]
