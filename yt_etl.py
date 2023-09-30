from googleapiclient.discovery import build
import pandas as pd
import time
import psycopg2 as pc
#import boto3 as bt

#request data from youtube api
def get_data():
    api_key = 'AIzaSyBKCuIpWGTiOM8YZiXicaeEskCv_jWoL7g'
    channel_ids = ['UCiP6wD_tYlYLYh3agzbByWQ', #fitness blender
                    'UCKcWSiffY8MpZ3NKav8LeRA', #motivationaldoc
                    'UCVbBtdw-_SCqGDs6-_awaDg', #cycling workout
                    'UCYC6Vcczj8v-Y5OgpEJTFBw', #fit tuber
                    'UCihUiDJzjyo2ov_qGtW33lw', #the yoga institute
                    'UCljIHM152GcJ9klhRKLfQ5A', #dance workout
                    ]
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.channels().list(part='snippet, contentDetails, statistics', id = ','.join(channel_ids))
    response = request.execute()
    return response
f1 = get_data()
#print(f1)

#transform raw data received
def transform_data(response):
    all_data = []
    for i in range(len(response['items'])):
        data = dict(
                    channel_id = str(response['items'][i]['id']),
                    channel_name = str(response['items'][i]['snippet']['title']),
                    description = str(response['items'][i]['snippet']['description']),
                    subscribers = int(response['items'][i]['statistics']['subscriberCount']),
                    views = int(response['items'][i]['statistics']['viewCount']),
                    total_videos = str(response['items'][i]['statistics']['videoCount'])
                    )
        all_data.append(data)
    return all_data
f2 = transform_data(f1)
#print(f2)

#load transformed data into aws object storage
def load_data(sdata):
    df = pd.DataFrame(sdata)
    timestamp = time.strftime('%d%m%Y_%H%M%S')
    filename = "YTdata_" + timestamp + ".csv"
    #df.to_csv(filename)
    df.to_csv("s3://yt-airflow-bucket/" + filename)
f3 = load_data(f2)
#print(f3)

#view as structured data in cloud database
def structure_data():
    # Create a connection to Amazon Redshift without SSL
    conn = pc.connect(
        host='yt-data.056665044923.ap-south-1.redshift-serverless.amazonaws.com',
        port=5439,
        user='***********',
        password='************',
        database='dev',
    )

    #aws S3 bucket information
    s3_bucket_name = 'yt-airflow-bucket'

    #redshift table to copy data
    rs_table = 'youtube'

    try:
        #create cursor object
        cur = conn.cursor()

        #S3 CSV file location
        s3_csv_files = f's3://{s3_bucket_name}/'

        #COPY command to load data from S3 to Redshift
        copy_command = f"""
        COPY {rs_table}
        FROM '{s3_csv_files}'
        IAM_ROLE 'arn:aws:iam::056665044923:role/s3-redshift-access'
        CSV
        DELIMITER ','
        IGNOREHEADER 1;
        """

        #execute COPY command
        cur.execute(copy_command)
        conn.commit()

        print("Data copied successfully from S3 to Redshift Serverless.")

        #close the cursor and connection
        cur.close()
        conn.close()

    except Exception as e:
        conn.rollback()
        print(f"Error: {str(e)}")

f4 = structure_data()
#print(structure_data())
