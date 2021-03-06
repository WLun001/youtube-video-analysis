import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

// date format from trending date
val DATE_FORMAT_INCOMPLETE = "yy.dd.mm"
//date format from trending date
val DATE_FORMAT = "yyyy-mm-dd"
val DESIRED_DATE_FORMAT = "dd/MM/yyyy"

/** reformat string date format */
def formatDate(s:String, currentDateFormat:String, dateFormat:String = DESIRED_DATE_FORMAT): String = {
	var simpleDateFormat:SimpleDateFormat = new SimpleDateFormat(currentDateFormat);
	var date:Date = simpleDateFormat.parse(s);
	return new SimpleDateFormat(dateFormat).format(date)
}

/** remote extra digits 
 *  2017-11-13T17:13:01.000Z or 2017-11-13T17:13:01.00Z to 2017-11-13
 */
def removeExtraDigit(s:String): String = {
	s.substring(0, s.indexOf("T"))
}

/** convert string date to desire date format */
def convertStringToDate(s: String, format: String = DESIRED_DATE_FORMAT): Date = {
    val dateFormat = new SimpleDateFormat(format)
    dateFormat.parse(s)
}

/** add day to given date*/
def addDay(date: Date, days: Int) : Date = {
  val dateAux = Calendar.getInstance()
  dateAux.setTime(date)
  dateAux.add(Calendar.DATE, days)
  return dateAux.getTime()
}


val formattedRDD = sqlContext.sql("select video_id, trending_date, channel_title, category_id, publish_time FROM parquet.`/user/cloudera/videos/videos.parquet`")
.map(x => 
	(x(0).asInstanceOf[String], x(1).asInstanceOf[String], x(2).asInstanceOf[String],
	 x(3).asInstanceOf[String], x(4).asInstanceOf[String]))
//get tuple (video_id, (trending_date, publish_date))
val formatter = formattedRDD.map(x =>
 (x._1, (formatDate(x._2, DATE_FORMAT_INCOMPLETE),formatDate(removeExtraDigit(x._5), DATE_FORMAT))))

//convert to Date
val trendingPublish = formatter.map(x => (x._1, (convertStringToDate(x._2._1), convertStringToDate(x._2._2))))

//filter if trending date is between 7-days after publish date
// get all the filtered videoIds and convert to list
val videoIds = trendingPublish.filter(x => x._2._1.before(addDay(x._2._2, 7))).keys.collect.toList

// get tuple of potentialChannel where those video become trend after publishing in 7 days. 
// benefit for advertisor to find youtuber for advertisement
// (category_id, channel_title)
val potentialChannel = formattedRDD.filter(x => videoIds.contains(x._1)).map(x => (x._4, x._3))

//read category name from text file and collect as key-value pairs
val category = sc.textFile("file:/home/cloudera/categoryid.txt").map(_.split(",")).map(line => (line(0), line(1)))

//match category_id with category name
val result = potentialChannel.join(category).map(x => (x._2._1, x._2._2)).sortBy(_._2)
result.collect

 //.coalesce(1).saveAsTextFile("newfile")
