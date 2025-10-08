import requests
import json
from datetime import date
import os
from airflow.sdk import task

maxresults = 50

@task
def get_playlist_id():
    API_KEY = os.environ.get("API_KEY")
    CHANNEL_HANDLE = os.environ.get("CHANNEL_HANDLE")
    try:

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        response.raise_for_status()

        data = response.json()

        # print(json.dumps(data, indent=4))

        channel_playlistid = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        # print(channel_playlistid)
        return channel_playlistid

    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_ids(playlist_id):
    API_KEY = os.environ.get("API_KEY")
    video_ids = []

    pageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxresults}&playlistId={playlist_id}&key={API_KEY}"

    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"
            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            pageToken = data.get('nextPageToken')
            if not pageToken:
                break

        return video_ids    

    except requests.exceptions.RequestException as e:
        raise e



@task
def extract_video_data(video_ids):
    API_KEY = os.environ.get("API_KEY")
    extracted_data = []

    def batch_list(video_id_lst, batch_size):
        for video_id in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[video_id:video_id + batch_size]

    try:
        for batch in batch_list(video_ids, maxresults):
            video_ids_str = ",".join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    'video_id': video_id,
                    'title': snippet.get('title'),
                    'publishedAt': snippet.get('publishedAt'),
                    'duration': contentDetails.get('duration'),
                    'viewCount': statistics.get('viewCount', None),
                    'likeCount': statistics.get('likeCount', None),
                    'commentCount': statistics.get('commentCount', None)
                }

                extracted_data.append(video_data)
                 
        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"

    with open(file_path, 'w', encoding='utf-8') as json_outfile:
        json.dump(extracted_data, json_outfile, ensure_ascii=False, indent=4)

    return file_path