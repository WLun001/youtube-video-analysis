// read file from local
val videos = sc.textFile("file:/home/cloudera/USvideos.txt")

// eliminate \n, drop header
val videosSplit = videos.map(_.split("\n"))
val videoRemoveHead = videosSplit.mapPartitionsWithIndex((index, it) => if(index == 0) it.drop(1) else it)

// element(0) is a row of data
val videosFlatten = videoRemoveHead.flatMap(x=>x)

// eliminate unwanted characters
val videosTabRemoved = videosFlatten.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
val ids = videosTabRemoved.map(x => x(0)).collect

// replace null description to "no description"
val videosTuple = videosTabRemoved.map(x => x match{
 case withDesc if(withDesc.size==16) => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15)); 
 case withoutDesc if(withoutDesc.size==15) => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),"no description"); 
 case error => ("dropThisTuple","","","","","","","","","","","","","","","") })

//remove duplicate data
val keyValue = videosTuple.map(x => 
	(x._1, (x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15, x._16)))

val reduceRDD= keyValue.map(tup =>
    (tup._1, tup)).reduceByKey { case (a, b) => a }.map(_._2)

val formattedrdd = reduceRDD.map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4,
 x._2._5, x._2._6, x._2._7, x._2._8, x._2._9, x._2._10, x._2._11, x._2._12, x._2._13, x._2._14, x._2._15))
