import boto3
import pandas as pd
import os
from dotenv import load_dotenv
import io
import ast

# Load environment variables from .env file
load_dotenv()

# Retrieve API keys and AWS credentials from environment variables
api_key = os.getenv("API_KEY").strip()
channel_id = os.getenv("CHANNEL_ID").strip()
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

# Initialize the S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

bucket_name = "andrew-huberman-podcast-analytics"
csv_key = "videos/videos.csv"

try:
    # Retrieve the CSV file from S3
    obj = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
    file_content = obj['Body'].read()

    # Load the content into a pandas DataFrame
    df = pd.read_csv(io.BytesIO(file_content))

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

# Set pandas display options to show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Clean and format specific columns
df_new_2['publishedAt'] = df_new_2['publishedAt'].str.slice(0, 10)
df_new_2['title'] = df_new_2['title'].str.strip()
df_new_2['description'] = df_new_2['description'].str.strip()

# Create a new column 'updated_title' by splitting the 'title' column by " | "
df_new_2['updated_title'] = df_new_2['title'].apply(lambda x: x.split("|")[0] if "|" in x else x)
df_new_2['guest'] = df_new_2['title'].apply(lambda x: x.split(":")[0] if ":" in x else "N/A")

df_new_2.drop(['title'], axis=1, inplace=True)
# Rename columns for better readability
df_new_2.rename(columns={
    'id': 'videoid',
    'thumbnails.maxres.url': 'thumbnailimageurl',
    'publishedAt': 'publishedat',
    'viewCount': 'viewscount',
    'likeCount': 'likescount',
    'commentCount': 'commentscount',
    'updated_title': 'title'
}, inplace=True)

# Change the data type of specific fields
df_new_2['viewscount'] = df_new_2['viewscount'].astype(int)
df_new_2['likescount'] = df_new_2['likescount'].astype(int)
df_new_2['commentscount'] = df_new_2['commentscount'].astype(int)
df_new_2['publishedat'] = pd.to_datetime(df_new_2['publishedat'])
df_new_2['videoid'] = df_new_2['videoid'].astype(str)
df_new_2['title'] = df_new_2['title'].astype(str)
df_new_2['guest'] = df_new_2['guest'].astype(str)
df_new_2['description'] = df_new_2['description'].astype(str)
df_new_2['duration'] = df_new_2['duration'].astype(str)
df_new_2['thumbnailimageurl'] = df_new_2['thumbnailimageurl'].astype(str)

# Select specific columns to create the final DataFrame
df_videos = df_new_2[['videoid', 'title','guest','description', 'publishedat', 'duration', 'thumbnailimageurl', 'viewscount', 'likescount', 'commentscount']]

# Save the cleaned DataFrame to a CSV buffer
csv_videos_buffer = io.StringIO()
df_videos.to_csv(csv_videos_buffer, index=False)

# Define the S3 key for the cleaned data
s3_key = 'clean_data/videos/videos.csv'

# Upload the cleaned CSV to S3
s3_client.put_object(
    Bucket=bucket_name,
    Key=s3_key,
    Body=csv_videos_buffer.getvalue()
)
