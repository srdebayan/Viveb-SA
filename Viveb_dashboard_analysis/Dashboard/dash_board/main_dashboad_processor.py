from datetime import datetime, timedelta

import pandas as pd
from .saga import load_saga_df,parse_saga_file
from .saga_instance import load_saga_instance_dfs,parse_saga_instance_file
from .saga_log import load_saga_instance_log_df,parse_saga_instance_log_file

# Load the DataFrames from CSV
class MainDashboard():
    def __init__(self):
        #lets load our dataframes
        try:
            self.saga_df = load_saga_df()
            self.saga_instance_df , self.saga_step_df = load_saga_instance_dfs()
            self.saga_instance_log_df = load_saga_instance_log_df()
            self.context  = {}
            # print(self.saga_df.columns,flush=True)
            # print(self.saga_instance_df.columns,flush=True)
            # print(self.saga_step_df.columns,flush=True)
            # print(self.saga_instance_log_df.columns,flush=True)
        except Exception as e:
            print(e,flush=True)  
    def get_status_options(self):
        return self.saga_instance_df.status.unique().tolist()
    
    def get_process_execution_stats(self):
        df=self.saga_instance_df
        total_executions = len(df)
        #print(self.saga_instance_df.columns,flush=True)
        #print(df.status.value_counts().index,flush=True)
        completed_successfully = df[df['status'] == 'COMPLETED']
        completed_with_error = df[df['status'] == 'FAILED']
        being_completed = df[(df['status'] == 'PENDING') |  (df['status'] == 'IN_PROGRESS')]  # Assuming 'PENDING'  status also means being completed
        compensated = df[df['status'] == 'COMPENSATED']
        
        # Calculating numbers and percentages
        num_completed_successfully = len(completed_successfully)
        perc_completed_successfully = (num_completed_successfully / total_executions) * 100
        num_completed_with_error = len(completed_with_error)
        perc_completed_with_error = (num_completed_with_error / total_executions) * 100
        num_being_completed = len(being_completed)
        perc_being_completed = (num_being_completed / total_executions) * 100
        num_compensated = len(compensated)  # Number of compensated executions
        perc_compensated = (num_compensated / total_executions) * 100  # Percentage of compensated executions

        stats= {
        'total_executions': total_executions,
        'num_completed_successfully': num_completed_successfully,
        'perc_completed_successfully': perc_completed_successfully,
        'num_completed_with_error': num_completed_with_error,
        'perc_completed_with_error': perc_completed_with_error,
        'num_being_completed': num_being_completed,
        'perc_being_completed': perc_being_completed,
        'num_compensated': num_compensated,  # Add to context
        'perc_compensated': perc_compensated,  # Add to context
        
    }    
        return stats

    def filter_dataframe(self, status, time_beg, time_end):
        df = self.saga_instance_df.copy()
        try:
            # Ensure df is a pandas DataFrame
            if not isinstance(df, pd.DataFrame):
                raise ValueError("Input 'df' is not a pandas DataFrame.")

            # Filter by status if the status list is not empty
            if status:
                if not isinstance(status, list):
                    raise ValueError("Status must be a list.")
                df = df[df['status'].isin(status)]
    
            # Convert time strings to timezone-aware datetime objects for comparison
            if 'createdAt_$date' in df.columns and 'updatedAt_$date' in df.columns:
                df['createdAt_$date'] = pd.to_datetime(df['createdAt_$date'], utc=True, errors='coerce')
                df['updatedAt_$date'] = pd.to_datetime(df['updatedAt_$date'], utc=True, errors='coerce')
            else:
                raise ValueError("Required date columns are not in DataFrame.")

            # Filter by time_beg if it's provided
            if time_beg:
                time_beg = pd.to_datetime(time_beg, utc=True, errors='coerce')
                if pd.isna(time_beg):
                    raise ValueError("Invalid format for time_beg.")
                df = df[df['createdAt_$date'] >= time_beg]

            # Filter by time_end if it's provided
            if time_end:
                time_end = pd.to_datetime(time_end, utc=True, errors='coerce')
                if pd.isna(time_end):
                    raise ValueError("Invalid format for time_end.")
                df = df[df['updatedAt_$date'] <= time_end]

            return df

        except Exception as e:
            print(f"Error occurred: {e}")
            return None