import org.apache.commons.lang.StringUtils
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader
import org.slf4j.{Logger, LoggerFactory}

//class is what reads from the file and sends it to the mapper as a key value
class AuthorRecordReader extends RecordReader[Text, Text] {
  val Log:Logger = LoggerFactory.getLogger(classOf[AuthorRecordReader])

  val lineRecordReader: LineRecordReader = new LineRecordReader()
  val key: Text = new Text()
  val value: Text = new Text()

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    lineRecordReader.initialize(split, context)


  }

  //convert an array to a string with * in between instead of using a var string
  def buildString(array: Array[String], built: String, start: Int, size: Int): String = {

    if (start >= size)
      return built

    if(array(start).contains("\""))
      {
        return buildString(array, built + (array(start).replaceAll("\"","")) + "*", start + 1, size)
      }

    return (buildString(array, built + array(start) + "*", start + 1, size))

  }

  override def nextKeyValue(): Boolean = {


    if (!lineRecordReader.nextKeyValue())
      return false

    //skips over lines that do not have authors
    while (lineRecordReader.getCurrentValue.toString.contains("<author>") == false) {
      Log.error("Skipping over line: "+lineRecordReader.getCurrentValue.toString+" as it does not contain authors")
      if (!lineRecordReader.nextKeyValue())
        return false
    }

    val xmlString: String = lineRecordReader.getCurrentValue().toString
    key.set(xmlString)



    val Type: String = StringUtils.substringBetween(xmlString, "<", " ")//returns the first argument of each xml line aka the type of input
    val authorArray: Array[String] = StringUtils.substringsBetween(xmlString, "<author>", "</author>")//returns an array of author names
    val authorString = buildString(authorArray, "", 0, authorArray.size)//builds the string to be passed to mapper
    val year:String = StringUtils.substringBetween(xmlString,"<year>","</year>")//gets year for histogram
    value.set(Type + "," + authorString + "," + year)
    return true;
  }


  override def getCurrentKey: Text = {
    return key
  }

  override def getCurrentValue: Text = {
    return value
  }

  override def getProgress: Float = {
    return lineRecordReader.getProgress
  }

  override def close(): Unit = {
    lineRecordReader.close()
  }
}