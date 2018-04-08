# Youtube Video Analysis
YouTube video analysis based on datasets on Kaggle

# How to run
### If havent start spark-shell
```spark-shell -i file.scala```
### If started spark-shell
```:load file.scala```

# File explanation
-  [setup.scala](setup.scala) - initial setup. Read from csv and clean date
-  [saveToParquet.scala](saveToParquet.scala) - save RDD to Parquet. Assume Parquet is created with Hive
-  [readFromParquet.scala](readFromParquet.scala) - read from Parquet after saved
-  [trending.scala](trending.scala) - Video Trending analysis
