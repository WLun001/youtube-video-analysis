import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import org.joda.time.Days
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

// get trending date and publish date in tuple
val formatter = formattedRDD.map(x => 
	(formatDate(x._2, DATE_FORMAT_INCOMPLETE),formatDate(removeExtraDigit(x._6), DATE_FORMAT)))

val trendingPublish = formatter.map(x => (convertStringToDate(x._1), convertStringToDate(x._2)))

val findTrendDate = trendingPublish.filter(x => x._1.before(addDay(x._2, 7)))

val filterDate = trendingPublish.filter(_._1.after(convertStringToDate("13/1/2018")))
val filterDate = trendingPublish.filter(_._1.after(convertStringToDate("13/1/2018")))




coalesce(1).saveAsTextFile("newfile")