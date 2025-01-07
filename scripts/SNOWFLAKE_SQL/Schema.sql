//Creating DataBase
CREATE OR REPLACE DATABASE youtube_ahc;

//creating storage integration object to establish a secure connection between AWS S3 bucket and snowflake.
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::211125530375:role/snowflake-access-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://andrew-huberman-podcast-analytics')
   COMMENT = 'integration object for secure access to AWS s3 bucket' ;

DESC INTEGRATION s3_integration;
   
//creating schema for s3 stage//
CREATE OR REPLACE SCHEMA youtube_ahc.stages;

//creating schema for file formats//
CREATE OR REPLACE SCHEMA youtube_ahc.file_formats;

// Create file format object
CREATE OR REPLACE FILE FORMAT youtube_ahc.FILE_FORMATS.csv_fileformat
    type = 'CSV'
    field_delimiter = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';



 // Create stage object with integration object & file format object for videos
CREATE OR REPLACE stage youtube_ahc.stages.videos_stage
    URL ='s3://andrew-huberman-podcast-analytics/clean_data/videos'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = youtube_ahc.FILE_FORMATS.csv_fileformat;
    
 // Create stage object with integration object & file format object for channel
CREATE OR REPLACE stage youtube_ahc.stages.channels_stage
    URL ='s3://andrew-huberman-podcast-analytics/clean_data/channeldetails'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = youtube_ahc.FILE_FORMATS.csv_fileformat;

 // Create stage object with integration object & file format object for coments
CREATE OR REPLACE stage youtube_ahc.stages.comments_stage
    URL ='s3://andrew-huberman-podcast-analytics/clean_data/comments/processedcomments/'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = youtube_ahc.FILE_FORMATS.csv_fileformat;

 // Create stage object with integration object & file format object for video topics
CREATE OR REPLACE stage youtube_ahc.stages.videotopics_stage
    URL ='s3://andrew-huberman-podcast-analytics/clean_data/videotopics/'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = youtube_ahc.FILE_FORMATS.csv_fileformat;
    
//Creating Channel Table
CREATE OR REPLACE TABLE youtube_ahc.PUBLIC.channel(
    channelid STRING PRIMARY KEY,
    channeltitle STRING,
    description STRING,
    channelcustomurl STRING,
    publisheddate DATE,
    channelcountry STRING,
    viewscount INT,
    subscriberscount INT,
    videoscount INT,
    channelthumbnailurl STRING
    
);

//Creating Videos table
CREATE OR REPLACE TABLE youtube_ahc.PUBLIC.videos(
    videoid STRING PRIMARY KEY,
    title STRING,
    guest STRING,
    description STRING,
    publisheddate DATE,
    duration STRING,
    thumbnailimageurl STRING,
    viewcount INT,
    likescount INT,
    commentscount INT

);

//Creating Comments table
CREATE OR REPLACE TABLE youtube_ahc.PUBLIC.comments(
    commentid STRING PRIMARY KEY,
    videoid STRING,
    publishedat DATE,
    textoriginal STRING,
    emotion STRING,
    sentimentnature STRING,
    authordisplayName STRING,
    authorprofileimageUrl STRING,
    authorchannelurl STRING,
    likeCount INT,
    totalreplycount INT 
);

CREATE OR REPLACE TABLE youtube_ahc.PUBLIC.topics(
    videoid STRING ,
    topic STRING
    );

COPY INTO youtube_ahc.PUBLIC.videos
FROM @youtube_ahc.stages.videos_stage
FILE_FORMAT = youtube_ahc.FILE_FORMATS.csv_fileformat
PATTERN = '.*\.csv$';


COPY INTO youtube_ahc.PUBLIC.channel
FROM @youtube_ahc.stages.channels_stage
FILE_FORMAT = youtube_ahc.FILE_FORMATS.csv_fileformat;

COPY INTO youtube_ahc.PUBLIC.comments
FROM @youtube_ahc.stages.comments_stage
PATTERN = '.*\.csv$'
ON_ERROR = 'CONTINUE';

COPY INTO youtube_ahc.PUBLIC.topics
FROM @youtube_ahc.stages.videotopics_stage
FILE_FORMAT = youtube_ahc.FILE_FORMATS.csv_fileformat
PATTERN = '.*\.csv$';

//Adding video categories, Altering some column values, and creating final videos table
CREATE OR REPLACE TABLE fn_VIDEOS AS (
WITH CTE AS (
    SELECT *,
        CASE 
            WHEN GUEST = 'N/A' THEN 'Solo'
            WHEN GUEST = 'Essentials' THEN 'Essentials'
            WHEN GUEST LIKE 'AMA%' THEN 'AMA'
            WHEN GUEST LIKE 'LIVE%' THEN 'LIVE EVENT'
            ELSE 'WITH GUEST'
        END AS videocategory,
        CONCAT('https://www.youtube.com/watch?v=',VIDEOID) AS VIDEOLINK
    FROM VIDEOS),
CTE1 AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER(PARTITION BY VIDEOID ORDER BY VIDEOID) AS RW
    FROM CTE  
)
SELECT 
    VIDEOID,
    TITLE,
    GUEST,
    VIDEOCATEGORY,
    VIDEOLINK,
    DESCRIPTION,
    PUBLISHEDDATE,
    DURATION,
    THUMBNAILIMAGEURL,
    VIEWCOUNT,
    LIKESCOUNT,
    COMMENTSCOUNT
FROM CTE1
WHERE RW = 1);

//Removing Duplicated and creating final Comments Table
CREATE TABLE fn_COMMENTS AS (
WITH CTE AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY COMMENTID ORDER BY COMMENTID) AS RW
FROM COMMENTS)
SELECT 
    COMMENTID,
    VIDEOID,
    PUBLISHEDAT,
    TEXTORIGINAL,
    EMOTION,
    SENTIMENTNATURE,
    AUTHORDISPLAYNAME,
    AUTHORPROFILEIMAGEURL,
    AUTHORCHANNELURL,
    LIKECOUNT,
    TOTALREPLYCOUNT
FROM CTE
WHERE RW = 1
);


