# Youtube Video Analysis
YouTube video analysis based on datasets on Kaggle

# How to run
### If havent start spark-shell
```spark-shell -i file.scala```
### If started spark-shell
```:load file.scala```

# File explanation
-  [setup.scala](setup.scala) - initial setup. Read from csv and clean date
-  [saveToParquet.scala](saveToParquet.scala) - save RDD to Parquet. Assume Parquet is created with Hive. <br>
```CREATE EXTERNAL TABLE videos(video_id STRING, trending_date STRING, title STRING, channel_title STRING, category_id STRING, publish_time STRING, tags STRING, views INT, likes INT, dislikes INT, comment_count INT, thumbnail_link STRING, comments_disabled BOOLEAN, ratings_disabled BOOLEAN, video_error_removed BOOLEAN, description STRING) STORED AS PARQUET LOCATION '/user/cloudera/labs';```
-  [readFromParquet.scala](readFromParquet.scala) - read from Parquet after saved
-  [trending.scala](trending.scala) - Video Trending analysis
