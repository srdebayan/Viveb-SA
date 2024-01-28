# In your dashboard/urls.py file

from django.urls import path


from django.conf import settings
from django.conf.urls.static import static
from . import views

urlpatterns = [
    path('dashboard/', views.main_dashboard, name='dashboard'),
    path('',views.main_dashboard,name="homepage"),
     path('upload/', views.file_upload, name='file_upload'),
     path('process_catalogue/',views.processes_catalogue,name="process_catalogue"),
     path('process/<process_id>',views.process_detail,name="process_detail")
]


if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)