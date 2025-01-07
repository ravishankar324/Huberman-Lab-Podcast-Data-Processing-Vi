## CHANNEL DETAILS ANALYSIS

import boto3
import pandas as pd
import os
from dotenv import load_dotenv
import io
# Load environment variables from .env file
load_dotenv()

api_key = os.getenv("API_KEY").strip()
channel_id = os.getenv("CHANNEL_ID").strip()
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")


#Initialize the S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region)


# Define the S3 bucket and CSV file key
bucket_name = "andrew-huberman-podcast-analytics"
csv_key = "channeldetails/channeldetails.csv"

try:
    # Get the object from S3
    obj = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
    file_content = obj['Body'].read()

    # Load the content into a pandas DataFrame
    df = pd.read_csv(io.BytesIO(file_content))

    # Display the first few rows of the DataFrame
    df.head()
except Exception as e:
    print(f"Error: {e}")

# Normalize the 'snippet' column and merge with the original DataFrame
df_snippet = pd.json_normalize(df['snippet'].apply(ast.literal_eval))
df_new = pd.concat([df, df_snippet], axis=1)
df_new.drop(columns=['snippet'], inplace=True)

# Normalize the 'statistics' column and merge with the DataFrame
df_statistics = pd.json_normalize(df_new['statistics'].apply(ast.literal_eval))
df_new_1 = pd.concat([df_new, df_statistics], axis=1)
df_new_1.drop(columns=['statistics'], inplace=True)

# Normalize the 'contentDetails' column and merge with the DataFrame
df_contentDetails = pd.json_normalize(df_new_1['contentDetails'].apply(ast.literal_eval))
df_new_2 = pd.concat([df_new_1, df_contentDetails], axis=1)
df_new_2.drop(columns=['contentDetails'], inplace=True)

# Clean and rename columns
df_new_2['publishedAt'] = df_new_2['publishedAt'].str.slice(0, 10)
df_new_2['description'] = df_new_2['description'].str.replace('\n', '')
df_new_2.rename(columns={
    'id': 'channelid',
    'title': 'channeltitle',
    'description': 'description',
    'customUrl': 'channelcustomurl',
    'publishedAt': 'publishedat',
    'country': 'channelcountry',
    'viewCount': 'viewscount',
    'subscriberCount': 'subscribercount',
    'videoCount': 'videoscount',
    'thumbnails.high.url': 'channelthumbnailurl'
}, inplace=True)

# Select relevant columns for the final DataFrame
df_channel = df_new_2[['channelid', 'channeltitle', 'description', 'channelcustomurl', 'publishedat', 'channelcountry', 'viewscount', 'subscribercount', 'videoscount', 'channelthumbnailurl']]

# Save the cleaned DataFrame to a CSV buffer
csv_channel_buffer = io.StringIO()
df_channel.to_csv(csv_channel_buffer, index=False)

# Define the S3 key for the cleaned data
s3_key = 'clean_data/channeldetails/channeldetails.csv'

# Upload the cleaned CSV to S3
s3_client.put_object(
    Bucket=bucket_name,
    Key=s3_key,
    Body=csv_channel_buffer.getvalue()
)
