import ast
from .saga import load_saga_df
from .saga_instance import load_saga_instance_dfs
from .saga_log import load_saga_instance_log_df




import pandas as pd

def calculate_step_execution_times():
    saga_df = load_saga_df()
    saga_instance_df , steps_df = load_saga_instance_dfs()
    saga_log_df = load_saga_instance_log_df()

    


    try:
        #lets check if all our files are present
        try:
            saga_df.all()
            saga_instance_df.all()
            saga_log_df.all()
            steps_df.all()
        except Exception as e:
            raise ValueError(f"some of the dataframes aren't present{e}")
        
        # Filter df2 for completed status
        completed_df = saga_instance_df[saga_instance_df['status'] == 'COMPLETED']

        # Group by 'saga_id' and calculate mean and std
        grouped = completed_df.groupby('saga_id')['execution_time_seconds']
        execution_stats = grouped.agg(['mean', 'std'])
        saga_df = saga_df.join(execution_stats, on='saga_id')
        # Save DataFrame as CSV
        output_csv_path = 'media/df/saga_file.csv'
        saga_df.to_csv(output_csv_path, index=False)
        print(f"DataFrame saved as {output_csv_path}",flush=True)

        # Convert to datetime
        saga_log_df['created_at'] = pd.to_datetime(saga_log_df['created_at'])
        saga_log_df['updated_at'] = pd.to_datetime(saga_log_df['updated_at'])

        # Sort the DataFrame
        saga_log_df = saga_log_df.sort_values(by=['sagaStepId', 'created_at'])

        # Calculate the running sum of execution times
        def running_sum(group):
            # Ensure the group has an even number of rows for pairwise calculation
            if len(group) % 2 != 0:
                group = group.iloc[:-1]  # Drop last row if odd number of rows
            time_diffs = group.iloc[1::2]['updated_at'].reset_index(drop=True) - group.iloc[::2]['created_at'].reset_index(drop=True)
            return time_diffs.sum().total_seconds()

        total_execution_time = saga_log_df.groupby('sagaStepId').apply(running_sum).reset_index(name='execution_time')

        steps_df = join_with_steps_df(steps_df,total_execution_time)
        print(steps_df.columns,flush=True)
        api_statstics_df=calculate_api_statistics(steps_df)
        steps_df=determine_step_performance(steps_df,api_statstics_df)
        saga_instance_df=determine_saga_performance(saga_instance_df,saga_df)
        
        steps_df.to_csv("media/df/saga_instance_steps.csv")
        api_statstics_df.to_csv("media/df/api_statistics.csv")
        saga_instance_df.to_csv("media/df/saga_instance_main.csv")

        calculate_overall_metrics(saga_df,saga_instance_df,steps_df)


    except Exception as e:
        print(f"Error in calculate_step_execution_times: {e}")
        return None

def join_with_steps_df(steps_df, total_execution_time):
    try:
        # Ensure the 'saga_step_id' in steps_df is the same type as 'sagaStepId' in total_execution_time
        steps_df['saga_step_id'] = steps_df['saga_step_id'].astype(total_execution_time['sagaStepId'].dtype)

        # Merging the DataFrames
        merged_df = steps_df.merge(total_execution_time, left_on='saga_step_id', right_on='sagaStepId', how='left')

        # Optionally, you can drop the extra 'sagaStepId' column after merging if it's redundant
        merged_df = merged_df.drop(columns=['sagaStepId'])

        return merged_df
    except Exception as e:
        print(f"Error in join_with_steps_df: {e}")
        return None



def calculate_api_statistics(steps_df):
    try:
        api_statistics_df = steps_df.groupby('name')['execution_time'].agg(['mean', 'std']).reset_index()
        return api_statistics_df
    except Exception as e:
        print(f"Error in calculate_api_statistics: {e}")
        return None

def determine_step_performance(steps_df, api_statistics_df):
    try:
        steps_df = steps_df.merge(api_statistics_df, on='name', how='left')

        def assign_status(row):
            if row['execution_time'] <= row['mean'] + 0.01 * row['std']:
                return 'green'
            elif row['execution_time'] <= row['mean'] + 0.1 * row['std']:
                return 'yellow'
            else:
                return 'red'

        steps_df['performance_status'] = steps_df.apply(assign_status, axis=1)
        return steps_df
    except Exception as e:
        print(f"Error in determine_step_performance: {e}")
        return None


def determine_saga_performance(saga_instance_df, saga_performance_statistics_df):
    try:
        saga_instance_df = saga_instance_df.merge(saga_performance_statistics_df, on='saga_id', how='left')

        def assign_saga_status(row):
            if row['execution_time_seconds'] <= row['mean'] + 0.01 * row['std']:
                return 'green'
            elif row['execution_time_seconds'] <= row['mean'] + 0.2 * row['std']:
                return 'yellow'
            else:
                return 'red'

        saga_instance_df['performance_status'] = saga_instance_df.apply(assign_saga_status, axis=1)
        return saga_instance_df
    except Exception as e:
        print(f"Error in determine_saga_performance: {e}")
        return None


import pandas as pd

from scipy import stats

# Assuming saga_df, saga_instance_df, and saga_steps_df are defined DataFrames


from scipy import stats

import pandas as pd

def calculate_overall_metrics(saga_df, saga_instance_df, saga_steps_df):
    try:
        # Convert 'createdAt_$date' to datetime
        saga_instance_df['createdAt_$date'] = pd.to_datetime(saga_instance_df['createdAt_$date'])

        for saga_id in saga_df['saga_id']:
            # Find the last 10 instances for the saga_id
            last_10_instances = saga_instance_df[saga_instance_df['saga_id'] == saga_id].nlargest(10, 'createdAt_$date')

            # Find the mode of the performance_status using Pandas' mode() method
            mode_performance_status = last_10_instances['performance_status'].mode()[0] if not last_10_instances['performance_status'].mode().empty else None

            # Update saga_df with mode_performance_status
            saga_df.loc[saga_df['saga_id'] == saga_id, 'mode_performance_status'] = mode_performance_status

            # Iterate through saga instances
            for _, instance in last_10_instances.iterrows():
                steps_ids = ast.literal_eval(instance['steps']) if isinstance(instance['steps'], str) else instance['steps']
                steps_data = saga_steps_df[saga_steps_df['saga_step_id'].isin(steps_ids)]

                mode_step_status = steps_data['status'].mode()[0] if not steps_data['status'].mode().empty else None
                saga_instance_df.loc[saga_instance_df['saga_instance_id'] == instance['saga_instance_id'], 'overall_apis_status'] = mode_step_status

                # Constructing columns for green, yellow, and red APIs
                green_apis = steps_data[steps_data['performance_status'] == 'green']['saga_step_id'].tolist()
                yellow_apis = steps_data[steps_data['performance_status'] == 'yellow']['saga_step_id'].tolist()
                red_apis = steps_data[steps_data['performance_status'] == 'red']['saga_step_id'].tolist()

                # Update saga_instance_df with green, yellow, and red apis
                saga_instance_df.loc[saga_instance_df['saga_instance_id'] == instance['saga_instance_id'], 'green_apis'] = str(green_apis)
                saga_instance_df.loc[saga_instance_df['saga_instance_id'] == instance['saga_instance_id'], 'yellow_apis'] = str(yellow_apis)
                saga_instance_df.loc[saga_instance_df['saga_instance_id'] == instance['saga_instance_id'], 'red_apis'] = str(red_apis)

        # Save the updated DataFrames back to their original paths
        saga_df.to_csv("media\df\saga_file.csv", index=False)
        saga_instance_df.to_csv("media\df\saga_instance_main_file.csv", index=False)

        print("DataFrames updated and saved successfully.")
    except Exception as e:
        print(f"Error in calculate_overall_metrics: {e}")

