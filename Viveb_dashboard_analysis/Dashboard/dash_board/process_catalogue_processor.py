from .saga import load_saga_df,parse_saga_file
from .saga_instance import load_saga_instance_dfs,parse_saga_instance_file
from .saga_log import load_saga_instance_log_df,parse_saga_instance_log_file



class ProcessCatalogue:
    def __init__(self):
        self.saga_df = load_saga_df()
        self.saga_instance_df , self.saga_step_df = load_saga_instance_dfs()
        self.saga_instance_log_df = load_saga_instance_log_df()
        self.context  = {}
        # print(self.saga_df.columns,flush=True)
        # print(self.saga_instance_df.columns,flush=True)
        # print(self.saga_step_df.columns,flush=True)
        # print(self.saga_instance_log_df.columns,flush=True)
    
    def get_process_catalogue(self):
        # Aggregate data (this is a simplified example, adapt as needed)
        df1= self.saga_df
        df2= self.saga_instance_df
        aggregated_data = []
        for _, row in df1.iterrows():
            saga_id = row['saga_id']
            related_executions = df2[df2['saga_id'] == saga_id]

            # Calculate statistics
            total_executions = len(related_executions)
            successful_executions = len(related_executions[related_executions['status'] == 'COMPLETED'])
            error_executions = len(related_executions[related_executions['status'] == 'FAILED'])
            ongoing_executions = len(related_executions[related_executions['status'] == 'IN_PROGRESS'])
            status = row['mode_performance_status']
            # Determine status (green, yellow, red)
            # Add your logic here based on your criteria

            # Append aggregated data
            aggregated_data.append({
                'name': row['name'],
                'url' : f'{saga_id}',
                'total_executions': total_executions,
                'successful_executions': successful_executions,
                'error_executions': error_executions,
                'ongoing_executions': ongoing_executions,
                'status': status
            })

        context = {'processes': aggregated_data}
        return context
        
    