import pandas as pd
import json
import io
from .saga_instance import load_saga_instance_dfs
saga_instance_df,steps_df = load_saga_instance_dfs()

def parse_saga_file(json_bytes, output_csv_path='media/df/saga_file.csv'):
    """
    Generate a Pandas DataFrame from JSON bytes and save it as a CSV.

    Args:
        json_bytes (bytes): The JSON file content in bytes.
        output_csv_path (str): The path to save the resulting CSV file.
    Returns:
        None
    """
    try:
        print(str(json_bytes),flush=True)
        # Decode the bytes to a string and load as JSON
        json_data = json.loads(json_bytes.decode('utf-8'))
        
        #print(json_data,flush=True)
        
        # Convert JSON to DataFrame
        df = pd.json_normalize(json_data, sep=".", record_path=None)

        # Renaming columns for clarity
        df.rename(columns={'_id.$oid': 'saga_id', 'createdAt.$date': 'createdAt', 'name': 'name'}, inplace=True)
        # # Filter df2 for completed status
        # completed_df = saga_instance_df[saga_instance_df['status'] == 'COMPLETED']

        # # Group by 'saga_id' and calculate mean and std
        # grouped = completed_df.groupby('saga_id')['execution_time_seconds']
        # execution_stats = grouped.agg(['mean', 'std'])
        # df = df.join(execution_stats, on='saga_id')
         # Save DataFrame as CSV
        df.to_csv(output_csv_path, index=False)
        print(f"DataFrame saved as {output_csv_path}",flush=True)

    except Exception as e:
        print(f"Error: {str(e)}",flush=True)


def load_saga_df(csv_path='media/df/saga_file.csv'):
    try:
        # Load the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_path)

        # You can perform any additional processing here if needed

        return df
    except FileNotFoundError:
        print(f"The file at {csv_path} was not found.")
        return None
    except Exception as e:
        print(f"An error occurred while loading the file: {e}")
        return None    
