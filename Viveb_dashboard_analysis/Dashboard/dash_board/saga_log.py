import pandas as pd
import json

def parse_saga_instance_log_file(json_bytes, output_csv_path='media/df/saga_instance_log.csv'):
    try:
        # Decode the bytes to a string and load as JSON
        json_data = json.loads(json_bytes.decode('utf-8'))

        # Normalize the main data
        main_df = pd.json_normalize(json_data, sep="_")
        # Renaming columns for clarity
        main_df.rename(columns={
            '_id_$oid': 'saga_instance_log_id',
            'createdAt_$date': 'created_at',
            'updatedAt_$date': 'updated_at',
            'sagaInstanceId_$oid': 'saga_instace_id'
        }, inplace=True)

        # Save the DataFrame to CSV
        main_df.to_csv(output_csv_path, index=False)
        print(f"DataFrame saved as {output_csv_path}")

    except Exception as e:
        print(f"Error: {str(e)}")

def load_saga_instance_log_df(csv_path='media/df/saga_instance_log.csv'):
    try:
        # Load the CSV file into a DataFrame
        saga_instance_df = pd.read_csv(csv_path)
        return saga_instance_df

    except FileNotFoundError as e:
        print(f"File not found: {str(e)}")
        return None
    except Exception as e:
        print(f"Error while loading the DataFrame: {str(e)}")
        return None