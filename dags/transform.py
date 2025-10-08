from datetime import date
import pandas as pd
import isodate
from airflow.sdk import task

def iso_to_ddhhmmss(iso_duration):
    duration = isodate.parse_duration(iso_duration)
    
    total_seconds = int(duration.total_seconds())
    
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{hours:02}:{minutes:02}:{seconds:02}"


@task(multiple_outputs=True)
def table_formatting(json_file_path):

    table = pd.read_json(json_file_path)
    table['Duration'] = table['duration'].apply(iso_to_ddhhmmss)
    table['publishedAt'] = pd.to_datetime(table['publishedAt']).dt.date
    table.rename(columns={'title': 'Video_Title', 'viewCount': 'Views', 'likeCount': 'Likes', 'commentCount': 'Comments','publishedAt': 'Published_Date'}, inplace=True)
    table.drop(columns=['duration','video_id'], inplace=True)
    table = table[['Video_Title', 'Duration', 'Views', 'Likes', 'Comments', 'Published_Date']]
    for col in ["Views", "Likes", "Comments"]:
        table[col] = pd.to_numeric(table[col], errors="coerce").fillna(0).astype(int)
    csv_file_path = f"./data/YT_data_{date.today()}.csv"
    table.to_csv(csv_file_path, index=False)
    columns = table.columns.tolist()
    return {"columns": columns, "csv_file_path": csv_file_path}

