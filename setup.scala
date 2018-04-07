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
