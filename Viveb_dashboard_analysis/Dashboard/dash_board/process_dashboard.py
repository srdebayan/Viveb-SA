



import matplotlib.pyplot as plt
import base64
from io import BytesIO
import pandas as pd
import numpy as np

def plot_execution_times(saga_instance_df, saga_df, saga_id):
    try:
        # Include all instances for the given saga_id
        filtered_instances = saga_instance_df[saga_instance_df['saga_id'] == saga_id]

        # Get mean and std for the saga_id
        saga_stats = saga_df[saga_df['saga_id'] == saga_id]
        if saga_stats.empty:
            print(f"No stats found for saga_id {saga_id}")
            return None

        mean = saga_stats.iloc[0]['mean']
        std = saga_stats.iloc[0]['std']

        # Define color mappings for status categories
        status_colors = {
            'COMPLETED': 'green',
            'COMPENSATED': 'lightgreen',
            'FAILED': 'red',
            'COMPENSATION_FAILED': 'lightcoral',
            'IN_PROGRESS': 'yellow',
            'PENDING': 'khaki',
            'COMPENSATING': 'gold'
        }

        # Prepare the plot
        plt.figure(figsize=(10, 6))
        
        # Plot each instance, using specific colors based on status category
        for status, instances in filtered_instances.groupby('status'):
            color = status_colors.get(status, 'grey')  # Default to grey if status not in dict
            plt.scatter(instances.index, instances['execution_time_seconds'], label=status, color=color)

        # Plot mean and standard deviation lines
        plt.axhline(y=mean, color='r', linestyle='-', label='Mean')
        plt.axhline(y=mean + std, color='g', linestyle='--', label='Mean + Std')
        plt.axhline(y=mean - std, color='g', linestyle='--', label='Mean - Std')

        plt.xlabel('Instance Index')
        plt.ylabel('Execution Time (seconds)')
        plt.title(f'Execution Times for Saga ID {saga_id}')
        plt.legend()

        # Save the plot to a BytesIO object and encode as base64 for embedding in HTML
        buffer = BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        image_png = buffer.getvalue()
        buffer.close()
        base64_encoded = base64.b64encode(image_png).decode('utf-8')

        return base64_encoded
    except Exception as e:
        print(f"Error in plot_execution_times: {e}")
        return None




def get_saga_instances_by_id(saga_instance_df, saga_id):
        # Filter saga_instance_df by saga_id
        filtered_instances = saga_instance_df[saga_instance_df['saga_id'] == saga_id]

        # Select important columns (adjust these as needed)
        columns_to_display = ['saga_instance_id', 'status', 'createdAt_$date', 'updatedAt_$date', 'execution_time_seconds']
        filtered_instances = filtered_instances[columns_to_display]

        return filtered_instances

import ast
from collections import Counter

def get_bottleneck_apis(saga_instance_df, saga_step_df, process_id):
    # Filter saga_instance_df by saga_id
    instances = saga_instance_df[saga_instance_df['saga_id'] == process_id]

    # Aggregate all red_apis into a list
    all_red_apis = []
    for apis in instances['red_apis'].dropna():
        try:
            # Convert the string representation of the list to an actual list
            apis_list = ast.literal_eval(apis)
            all_red_apis.extend(apis_list)
        except ValueError:
            # Handle cases where ast.literal_eval fails to parse the string
            pass

    # Find the most common API IDs
    most_common_apis = Counter(all_red_apis).most_common()

    # Get the names of the most common APIs
    bottleneck_apis = saga_step_df[saga_step_df['saga_step_id'].isin([api_id for api_id, _ in most_common_apis])]['name'].unique().tolist()

    return bottleneck_apis
