
//Creating Perceantge for sentimentnature analaysis
CREATE OR REPLACE TABLE sentimentnature_per AS
WITH CTE AS (
SELECT VIDEOID, SENTIMENTNATURE, ROUND(COUNT(SENTIMENTNATURE)/ SUM(COUNT(SENTIMENTNATURE)) OVER(PARTITION BY VIDEOID) * 100,2) AS percentage_sentimentnature
FROM fn_COMMENTS
WHERE SENTIMENTNATURE = 'positive' OR SENTIMENTNATURE = 'negative' OR SENTIMENTNATURE = 'neutral'
GROUP BY VIDEOID, SENTIMENTNATURE
ORDER BY VIDEOID)
SELECT VIDEOID, 
    SUM(CASE WHEN SENTIMENTNATURE = 'positive' THEN PERCENTAGE_SENTIMENTNATURE ELSE 0 END) AS positivecommentspercentage, 
    SUM(CASE WHEN SENTIMENTNATURE = 'negative' THEN PERCENTAGE_SENTIMENTNATURE ELSE 0 END) AS negativecommentspercentage, 
    SUM(CASE WHEN SENTIMENTNATURE = 'neutral' THEN PERCENTAGE_SENTIMENTNATURE ELSE 0 END) AS neutralcommentspercentage
FROM CTE
GROUP BY VIDEOID
ORDER BY VIDEOID;


//Creating a cumulative table for flask to query based on user requests
CREATE OR REPLACE TABLE FN_BIGTABLE AS 
SELECT  
    a.VIDEOID AS VIDEOID,
    LOWER(a.TITLE) AS TITLE, 
    LOWER(a.DESCRIPTION) AS DESCRIPTION,
    a.THUMBNAILIMAGEURL AS THUMBNAILIMAGEURL,
    a.VIDEOLINK AS VIDEOLINK,
    LOWER(a.VIDEOCATEGORY) AS VIDEOCATEGORY,
    LOWER(a.GUEST) AS GUEST,
    a.PUBLISHEDDATE AS PUBLISHEDDATE,
    a.DURATION AS DURATION,
    a.VIEWCOUNT AS VIEWCOUNT,
    a.LIKESCOUNT AS LIKESCOUNT,
    a.COMMENTSCOUNT AS COMMENTSCOUNT,
    b.POSITIVECOMMENTSPERCENTAGE AS POSITIVECOMMENTSPERCENTAGE,
    b.NEGATIVECOMMENTSPERCENTAGE AS NEGATIVECOMMENTSPERCENTAGE,
    b.NEUTRALCOMMENTSPERCENTAGE AS NEUTRALCOMMENTSPERCENTAGE,
    LOWER(c.TOPIC) AS TOPIC   
FROM fn_videos AS a
LEFT JOIN sentimentnature_per AS b
ON a.VIDEOID = b.VIDEOID
LEFT JOIN topics AS c
ON a.VIDEOID = c.VIDEOID;



//Best Video in all Topics
CREATE OR REPLACE TABLE FN_RK1_TABLE AS
WITH CTE AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY TOPIC ORDER BY VIEWCOUNT DESC, LIKESCOUNT DESC) AS rank 
FROM FN_BIGTABLE)

SELECT * 
FROM CTE WHERE rank = 1;


