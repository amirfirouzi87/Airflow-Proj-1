from airflow.sdk import DAG
import pendulum
from datetime import timedelta, datetime
from api.video_info import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from transform import table_formatting
from load import create_table, load_YT_data

local_tz = pendulum.timezone("Europe/Helsinki")

default_args = {
    'owner': 'data-engineers',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'data@engineers.com',
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=60),
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    'end_date': datetime(2030, 6, 1, tzinfo=local_tz),
}


with DAG(
    dag_id="Youtube_ETL_DAG",
    default_args=default_args,
    description="A DAG to extract YouTube video stats and save to JSON",
    catchup=False,
) as dag:

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(video_data)
    format_table = table_formatting(save_to_json_task)
    create_table_task = create_table(format_table["columns"])
    load_YT_data_task = load_YT_data(format_table["csv_file_path"])
    # Define Dependencies
    playlist_id >> video_ids >> video_data >> save_to_json_task >> format_table >> create_table_task >> load_YT_data_task
