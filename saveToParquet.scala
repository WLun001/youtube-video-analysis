import org.apache.spark.sql.SaveMode
//assume the parquet table is created on Hive
val df = formattedRDD.toDF("video_id", "trending_date", "title", "channel_title", "category_id",
 "publish_time", "tags", "views", "likes", "dislikes", "comment_count", "thumbnail_link",
  "comments_disabled", "ratings_disabled", "video_error_removed", "description")

// write dataframe to parquet table in hdfs
df.write.format("parquet").save("/user/cloudera/videos/videos.parquet")
