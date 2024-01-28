from django.shortcuts import redirect, render
from .preprocessing import *
import pandas as pd

from datetime import datetime, timedelta, timezone
from .main_dashboad_processor import MainDashboard
from .process_catalogue_processor import ProcessCatalogue
from .calculations import calculate_step_execution_times


main_dashboard_processor= MainDashboard()
processes_catalogue_processor = ProcessCatalogue()
from .saga import load_saga_df
from .saga_instance import load_saga_instance_dfs
from .process_dashboard import plot_execution_times,get_saga_instances_by_id,get_bottleneck_apis




def process_detail(request, process_id):
    saga_df = load_saga_df()
    saga_instance_df, saga_step_df = load_saga_instance_dfs()
    img = plot_execution_times(saga_instance_df, saga_df, process_id)
    saga_instances = get_saga_instances_by_id(saga_instance_df, process_id)
    bottleneck_apis = get_bottleneck_apis(saga_instance_df, saga_step_df, process_id)

    context = {
        'image_string': img,
        'process_id': process_id,
        'saga_instances': saga_instances.to_html(classes='table table-striped', index=False),
        'bottleneck_apis': bottleneck_apis
    }
    return render(request,'dash_board/process_detail.html',context=context)
    



def processes_catalogue(request):
    context = processes_catalogue_processor.get_process_catalogue()
    return render(request, 'dash_board/process_catalogue.html', context)




def create_saga_hyper_link(value):
    # Assuming 'value' is the URL or a part of it
    return f'<a href="{value}">{value}</a>'



def main_dashboard(request):
    context = main_dashboard_processor.get_process_execution_stats()  # Common context
    context['status_options'] = main_dashboard_processor.get_status_options()

    if request.method == 'POST':
        try:
            # Parsing selected values from the dropdown
            selected_statuses = request.POST.getlist('statuses')

            # Parsing values from the date-time pickers
            start_time_str = request.POST.get('start_time')
            end_time_str = request.POST.get('end_time')

            # Convert time strings to datetime objects, handling invalid formats
            start_time = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M') if start_time_str else None
            end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M') if end_time_str else None
            # Extracting 'hours before' value from the form
            hours_before_str = request.POST.get('hours_before')
            if hours_before_str:
                try:
                    # Convert the input to an integer
                    hours_before = int(hours_before_str)
                
                    # Calculate the time range based on the current time and 'hours before'
                    end_time = datetime.now(tz=timezone.utc)  # Adjust timezone as needed
                    start_time = end_time - timedelta(hours=hours_before)
                except ValueError:
                    # Handle the case where 'hours_before' is not a valid integer
                    pass  # Add error handling logic here


            # Filter data and convert to HTML table
            processes = main_dashboard_processor.filter_dataframe(status=selected_statuses, time_beg=start_time, time_end=end_time)
            processes = processes.to_html() if processes is not None else 'No data available'

            context['processes'] = processes

        except ValueError as e:
            # Handle specific error (e.g., date parsing error)
            context['error'] = f"Invalid input: {e}"
        except Exception as e:
            # Handle any other unforeseen errors
            context['error'] = f"An error occurred: {e}"

    # Render the template with context
    return render(request, 'dash_board/main_dashboard.html', context)


def home_page(request):
    return render (request,'dash_board/base.html')



from django.core.files.storage import FileSystemStorage
from .models import VivebFile
from .file_processing import save_files, load_files





from .saga import parse_saga_file
from .saga_instance import parse_saga_instance_file
from .saga_log import parse_saga_instance_log_file
from .calculations import calculate_step_execution_times
def save_to_df(uploaded_files):
    try:
        for file_type, file in uploaded_files.items():
            if file is not None:
                if file_type == 'saga':
                    # Assuming parse_saga_file is your custom function for 'saga' file type
                    print("in saga",type(file),flush=True)
                    file.seek(0)
                    parse_saga_file(file.read())
                elif file_type == 'saga_instance':
                    # Using the parse_saga_instance_file function
                    file.seek(0)
                    parse_saga_instance_file(file.read())
                elif file_type == 'saga_log':
                    # Handle saga_log file or skip if not implemented
                    file.seek(0)
                    parse_saga_instance_log_file(file.read())
                else:
                    print(f"Unknown file type: {file_type}")
        calculate_step_execution_times()

    except Exception as e:
        print(f"An error occurred: {e}")



def file_upload(request):
    if request.method == 'POST':
        uploaded_files = {
            'saga': request.FILES.get('saga_file'),
            'saga_instance': request.FILES.get('saga_instance_file'),
            'saga_log': request.FILES.get('saga_log_file')
        }
        #lets save the files to DB
        save_files(uploaded_files)
        #also we should convert them to a dataframe
        save_to_df(uploaded_files)
        
        return redirect('file_upload')  # Redirect to a success page or the same page

    saga_files = load_files()
    return render(request, 'dash_board/file_upload.html', {'saga_files': saga_files})
       