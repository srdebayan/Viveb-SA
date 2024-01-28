from django.core.exceptions import ValidationError
from .models import VivebFile

def save_files(files):
    """
    Saves files to the VivebFile model.
    :param files: Dictionary with keys as file types and values as file objects.
    :return: None
    """
    for file_type, file_obj in files.items():
        if file_obj==None:continue
        if file_type in dict(VivebFile.FILE_TYPES) :
            viveb_file = VivebFile(file=file_obj, file_type=file_type)
            try:
                viveb_file.save()
            except Exception as e:
                # Handle exceptions like saving errors, etc.
                raise ValidationError(f"Error saving file: {e}")
        else:
            raise ValidationError("Invalid file type")

def load_files():
    """
    Loads the most recent files of each type from the VivebFile model.
    :return: Dictionary mapping file types to file objects.
    """
    recent_files = {}
    for file_type, _ in VivebFile.FILE_TYPES:
        try:
            recent_file = VivebFile.objects.filter(file_type=file_type).latest('id')
            recent_files[file_type] = recent_file.file
        except VivebFile.DoesNotExist:
            recent_files[file_type] = None
    return recent_files
