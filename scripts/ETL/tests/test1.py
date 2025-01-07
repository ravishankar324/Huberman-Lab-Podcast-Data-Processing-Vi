import os
import sys

# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, trim, split, udf
from unicodedata import normalize
from pyspark.sql.types import StringType
from pyspark import TaskContext
from dotenv import load_dotenv
from openai import OpenAI
import pandas as pd
import os
import io
import re
import boto3
import openai
import time
import socket
import logging
import sys
import argparse


# Function to filter files by suffix
def filter_by_suffix(key, start, end):
    match = re.search(r'_(\d+)\.csv$', key)
    if match:
        suffix = int(match.group(1))
        return start <= suffix < end
    return False

def write_consolidated_log_to_s3(bucket, key, new_logs):
    # Step 1: Read existing content from S3
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        existing_content = response['Body'].read().decode('utf-8')
        print(f"Existing content read from S3: {key}")
    except s3_client.exceptions.NoSuchKey:
        # If the file doesn't exist, initialize with an empty string
        print(f"No existing file found at {key}. Starting fresh.")
        existing_content = ""

    # Step 2: Append new logs to existing content
    consolidated_logs = existing_content + "\n" + "\n".join(new_logs)
    # Upload the consolidated log to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=consolidated_logs.encode('utf-8')
    )
    print(f"Consolidated log written to S3 at {key}")

# Define a Pandas UDF for batch sentiment analysis
@pandas_udf(StringType())
def analyze_emotions_in_batch(comments_series):
      
    import openai
    from openai import OpenAI
    import pandas as pd

    # Initialize an empty list to store emotions
    emotions = []

    # Convert the Pandas Series to a list
    comments_list = comments_series.tolist()
    partition_id = TaskContext.get().partitionId()
    apikeys= broadcast_api_keys.value[partition_id % len(broadcast_api_keys.value)]
    
    # Define the system context with explicit structure instructions
    system_message = (
    "You are a sentiment and emotion analysis assistant specialized in analyzing YouTube comments. "
    "The following comments are from viewers of Andrew Huberman's podcast YouTube channel and are replies to his podcast videos. "
    "Andrew Huberman is a neuroscientist and educator known for his in-depth discussions about neuroscience, health, and wellness. "
    "These comments are responses to his podcast episodes and may contain feedback, appreciation, questions, or other emotional expressions. "
    "\n\n"
    "Your task is to analyze each comment and determine the specific emotion expressed and whether the sentiment is positive, negative, or neutral. "
    "The specific emotion must be only one among of the following these which suits best to the comment: admiration, amusement, anger, annoyance, approval, caring, confusion, "
    "curiosity, desire, disappointment, disapproval, disgust, embarrassment, excitement, fear, gratitude, grief, "
    "joy, love, nervousness, optimism, pride, realization, relief, remorse, sadness, surprise, neutral. "
    "\n\n"
    "Follow this format strictly:\n"
    "Comment 1: specific emotion, positive/negative/neutral\n"
    "Comment 2: specific emotion, positive/negative/neutral\n"
    "...\n\n"
    "Example:\n"
    "Input: 'Really helpful podcast!'\n"
    "Output: Comment 1: gratitude, positive\n"
    "In this case, the emotion is gratitude, and the sentiment is positive.\n\n"
    "Input: 'Today's weather is great!'\n"
    "Output: Comment 1: joy, positive\n"
    "In this case, the emotion is joy, and the sentiment is positive.\n\n"
    "Always keep in mind that these comments are related to Andrew Huberman's podcast. Provide your analysis in the specified format."
    )
     # Fetch API keys from the broadcast variable
    partition_id = TaskContext.get().partitionId()

    # Batch size for processing
    batch_size = 20  

    # Maximum number of retries
    max_retries = 3  

    # Process comments in batches
    for i in range(0, len(comments_list), batch_size):
        batch_comments = comments_list[i:i + batch_size]
        retries = 0
        success = False
     
    # Decode the key from bytes
        while not success and retries < max_retries:
            try:
                

                # Get the partition ID dynamically from the Spark context
                partition_id = TaskContext.get().partitionId()
                
                print(f"partiion index {partition_id}")
                
            
                # Prepare the batched prompt
                print(f"Processing key for partition {partition_id} batch {i} : {apikeys}")
                batched_comments = "\n".join([f"Comment {idx+1}: {comment}" for idx, comment in enumerate(batch_comments)])
                prompt = (
                    f"Analyze the sentiment of the following {len(batch_comments)} comments:\n{batched_comments}\n\n"
                    "Provide the emotion for each comment in the format:\n"
                    "Comment 1: Emotion, positive/negative/neutral\n"
                    "Comment 2: Emotion, positive/negative/neutral\n"
                    "...\n"
                    "\n"
                    "Example:\n"
                    "Input: 'Today's weather is great!'\n"
                    "Output: Comment 1: joy, positive\n\n"
                    "Provide the response strictly in the format described above."
                )
                client = OpenAI(api_key=apikeys)
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system_message},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0
                )
                
                content = response.choices[0].message.content.strip()
                lines = content.split('\n')
                print(f"try {retries} for {partition_id} for batch {i} successful")

                # Extract emotions from the response
                batch_emotions = []
                for line in lines:
                    if ':' in line:
                        # Expected format: 'Comment X: Emotion, Sentiment'
                        parts = line.split(':', 1)
                        emotion = parts[1].strip()
                        batch_emotions.append(emotion)
                    else:
                        batch_emotions.append('Error-Unable to parse emotion')

                # Ensure the batch_emotions list matches the number of comments in the batch
                if len(batch_emotions) == len(batch_comments):
                    emotions.extend(batch_emotions)
                    success = True  # Mark the attempt as successful
                else:
                    raise ValueError("Mismatch between input and output sizes.")

            except Exception as e:
                print(f"Error on attempt {retries} for partition {partition_id} for batch {i}: {str(e)}")
                retries += 1
                if retries < max_retries:
                    time.sleep(2 ** retries)  # Exponential backoff
                else:
                    # Assign error messages if retries are exhausted
                
                    emotions.extend(["Error-Unable to parse emotion"] * len(batch_comments))
        
         # Return the API key to the queue
        print(f"{partition_id} leaving key: {apikeys}")

          # Delay of 500ms (adjust based on performance and rate limits)
        time.sleep(1)

    # Return a Pandas Series of emotions
    return pd.Series(emotions)
# Initialize boto3 S3 client
s3_client = boto3.client(
        "s3",
    )

# S3 bucket details
bucket_name = "andrew-huberman-podcast-analytics"
file_key_env = "env/.env"
local_path = "/tmp/.env"
prefix = "clean_data/comments/notprocessedcomments/__RAXBLt1iM_155/__RAXBLt1iM_155.csv"
# Download the .env file from S3
s3_client.download_file(bucket_name, file_key_env, local_path)

# Load the .env file
load_dotenv(local_path)

# Fetch AWS credentials from the environment
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_region = os.environ.get("AWS_REGION")

# Check if all necessary AWS credentials and region are available
if not all([aws_access_key_id, aws_secret_access_key, aws_region]):
    raise ValueError("Missing AWS credentials or region in .env file")

# Initialize SparkSession
spark = SparkSession.builder \
        .appName("ReadParquetFromS3") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.524") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "50") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10") \
        .config("spark.hadoop.fs.s3a.retry.limit", "10") \
        .config("spark.hadoop.fs.s3a.max.connections", "20") \
        .config("spark.hadoop.fs.s3a.socket.timeout", "600000") \
        .config("spark.python.worker.reuse", "true") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.network.timeout", "120s") \
        .getOrCreate()


# Fetching  API keys
api_keys = [os.environ.get("OPENAI_API_KEY"),
    os.environ.get("OPENAI_API_KEY_1"),
    os.environ.get("OPENAI_API_KEY_2"),
    os.environ.get("OPENAI_API_KEY_3"),
    os.environ.get("OPENAI_API_KEY_4"),
    os.environ.get("OPENAI_API_KEY_5"),
    os.environ.get("OPENAI_API_KEY_6"),
    os.environ.get("OPENAI_API_KEY_7"),
    os.environ.get("OPENAI_API_KEY_8"),
    os.environ.get("OPENAI_API_KEY_9"),
    os.environ.get("OPENAI_API_KEY_10"),
    os.environ.get("OPENAI_API_KEY_11"),
    os.environ.get("OPENAI_API_KEY_12"),
    os.environ.get("OPENAI_API_KEY_13"),
    os.environ.get("OPENAI_API_KEY_14"),
    os.environ.get("OPENAI_API_KEY_15"),
    os.environ.get("OPENAI_API_KEY_16"),
    os.environ.get("OPENAI_API_KEY_17"),
    os.environ.get("OPENAI_API_KEY_18"),
    os.environ.get("OPENAI_API_KEY_19"),
    os.environ.get("OPENAI_API_KEY_20"),
    os.environ.get("OPENAI_API_KEY_21"),
    os.environ.get("OPENAI_API_KEY_22"),
    os.environ.get("OPENAI_API_KEY_23")]
# List to store log messages
log_entries = []
# Broadcast API keys to all Spark executors
broadcast_api_keys = spark.sparkContext.broadcast(api_keys)
# List files in the S3 bucket
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Extract file keys from the response
# Filter files based on the provided suffix range
file_keys = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]

"""
# Check if there are any files to process
if not file_keys:
    raise ValueError("No files found in the specified directory.")

# Sort the selected keys by suffix
file_keys.sort(key=lambda x: int(re.search(r'_(\d+)\.parquet$', x).group(1)))
"""
# Construct S3 paths for the selected files
selected_paths = f"s3a://{bucket_name}/{prefix}"


df = spark.read.csv(selected_paths)
        
print(f"Initial partitions: {df.rdd.getNumPartitions()}")
df = df.repartition(4)
print(f"Repartitioned partitions: {df.rdd.getNumPartitions()}")
    
df.show()
"""
    # Select relevant columns
    df = df.select(
            "commentid",
            "videoid", 
            "publishedat", 
            "textoriginal",
            "authordisplayName", 
            "authorprofileimageUrl", 
            "authorchannelurl",
            "likeCount", 
            "totalreplycount"
        )

    comments_with_emotions_df = df.withColumn(
        "emotion",
        analyze_emotions_in_batch(col("textoriginal"))
        )

    # Split the emotion column into specific emotion and sentiment
    comments_with_emotions_df = comments_with_emotions_df.withColumn(
        "emotion_split",
        split(col("emotion"), ",")
        )

     # Select and rename the split columns
    comments_with_emotions_df = comments_with_emotions_df.select(
            col("commentid"),
            col("videoid"),
            col("publishedat"),
            col("textoriginal"),
            col("authordisplayName"),
            col("authorprofileimageUrl"),
            col("authorchannelurl"),
            col("likeCount"),
            col("totalreplycount"),
            col("emotion_split").getItem(0).alias("emotion"),
            col("emotion_split").getItem(1).alias("sentimentnature")
        )
        
    # Select final columns for the DataFrame
    df_channelcomments = comments_with_emotions_df.select(
            "commentid",
            "videoid", 
            "publishedat", 
            "textoriginal",
            "emotion", 
            "sentimentnature", 
            "authordisplayName", 
            "authorprofileimageUrl", 
            "authorchannelurl",
            "likeCount", 
            "totalreplycount"
        )

    # Show results
    

    # Upload to S3
    # Append processed and unprocessed comments to S3
    # Process and write data to S3
    s3_key_notprocessed = f's3a://{bucket_name}/clean_data/comments/notprocessedcomments/{pre}/'
    s3_key_processed = f's3a://{bucket_name}/clean_data/comments/processedcomments/{pre}/'

    df_channelcomments.cache()
    # Filter and write not processed comments
    total_rows = df_channelcomments.count()
    df_not_processed = df_channelcomments.filter(col("emotion") == "Error-Unable to parse emotion")
    df_processed = df_channelcomments.filter(col("emotion") != "Error-Unable to parse emotion")
    rows_not_procesed = df_not_processed.count()
    rows_procceed = df_processed.count()
    if df_not_processed.count() > 0:
        df_not_processed.repartition(1).write.mode("overwrite").option("header", "true").csv(s3_key_notprocessed)  


    # Filter and write processed comments
    if df_processed.count() > 0:
        df_processed.repartition(1).write.mode("append").option("header","true").csv(s3_key_processed)
        
    df_channelcomments.unpersist()

    # Log upload success 
    upload_success = f"Uploaded file to S3 successfully: {pre}, total rows: {total_rows}, processed rows: {rows_procceed}"

    # Example log entry creation
    upload_success = f"Uploaded file to S3 successfully: {pre}, total rows: {total_rows}, processed rows: {rows_procceed}"

    # Append log message to the list
    log_entries.append(upload_success)
    print(upload_success)
    
# At the end of the job
write_consolidated_log_to_s3(
    bucket_name,
    "clean_data/comments/logs/uploadedfiles.txt",
    log_entries
)
"""
spark.stop()
