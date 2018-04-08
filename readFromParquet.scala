//assume there is data in parquet
val parquetRDD = sqlContext.sql("SELECT * FROM parquet.`/user/cloudera/videos/videos.parquet`")
parquetrdd.count

//cast to relevant data type
val castedRDD = parquetrdd.map(x => 
	(x(0).asInstanceOf[String], x(1).asInstanceOf[String], x(2).asInstanceOf[String],
	 x(3).asInstanceOf[String], x(4).asInstanceOf[String], x(5).asInstanceOf[String], 
	 x(6).asInstanceOf[String], x(7).asInstanceOf[String], x(8).asInstanceOf[String], 
	 x(9).asInstanceOf[String], x(10).asInstanceOf[String], x(11).asInstanceOf[String], 
	 x(12).asInstanceOf[String], x(13).asInstanceOf[String], x(14).asInstanceOf[String], x(15).asInstanceOf[String]))
//continue with Spark SQL