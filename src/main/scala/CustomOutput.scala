import java.io.{DataOutputStream, File}
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.util.Progressable
import org.apache.commons.lang.StringUtils
import org.apache.commons.text.StringEscapeUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.TreeMap
class CustomOutput extends FileOutputFormat[Text,Author ]{
  val Log:Logger = LoggerFactory.getLogger(classOf[CustomOutput])
var binsYearCount:TreeMap[Int,Int] = new TreeMap[Int,Int]
  class MyWriter(dataOutputStream: DataOutputStream) extends RecordWriter[Text,Author]{
    val out:DataOutputStream = dataOutputStream

    var printOnce = false
    override def write(key: Text, value: Author): Unit = {

      if(key.toString.contains("#"))
        {
          out.writeBytes(value.words.toString)
          return
        }
      //histograms give keys like this so they will print at the top of csv for better user experience follows ascii order
      if(key.toString.contains("\"A")||key.toString.contains("\"B")||key.toString.contains("\"C")||key.toString.contains("\"D"))
        {
          if(key.toString.contains("\"A")) {
            out.writeBytes("Year Count histogram\n")
            Log.info("Beginning print of year histogram")
          }
          else if(key.toString.contains("\"D")) {
            out.writeBytes("Author Count histogram\n")
            Log.info("Beginning print of Author Count histogram")
          } else if(key.toString.contains("\"B")) {
            out.writeBytes("Journal Author Count histogram\n")
            Log.info("Beginning Journal histogram")
          } else if(key.toString.contains("\"C")) {
            out.writeBytes("Conference Author Count histogram\n")
            Log.info("Beginning Conference histogram")
          }

          //The same structure was made to create all histograms so they all can be printed here
          //all values were stored in strings such as bin:int we will add these to binsCount so they get sorted
          //then we will write them to the file in order for a proper histogram (minus the bars of course)
          val string:Array[String]= value.words.toString.split(',').filter(z=>(!z.isEmpty))
          string.foreach(x=>{
            val y:Array[String]=x.split(":")
            if(y.length==2) {
              if (!x.isEmpty) {
                if (binsYearCount.contains(y(0).trim.toInt)) {
                  val x: Int = binsYearCount(y(0).trim.toInt) + y(1).trim.toInt
                  binsYearCount += (y(0).trim.toInt -> x)
                }
                else {
                  binsYearCount += (y(0).trim.toInt -> y(1).trim.toInt)
                }
              }
            }

          })




       binsYearCount.foreach(x=>out.writeBytes(x._1+":,"+x._2+"\n"))
          out.writeBytes("\n")
          binsYearCount = new TreeMap[Int,Int]
          if(key.toString.contains("\"Z")) {

          }
          return
        }

      val keyS:String = key.toString
      if(keyS.contains("\"book")||keyS.contains("\"article")||keyS.contains("\"inproceedings")||
        keyS.contains("\"proceedings")||keyS.contains("\"incollection") ||keyS.contains("\"phdthesis")||
        keyS.contains("\"mastersthesis")||keyS.contains("\"www")||keyS.contains("\"all"))
        {
          val split = value.words.toString.split(':')
          out.writeBytes(keyS.replace("\"","")+",Max: "+ split(0)+",Mean: "+split(1)+"\n")
          return
        }

      if(!printOnce)
        {
          out.writeBytes("Author,Authorship,numberOfPublications,MaxCoauthors,MedianCoauthors,MinCoauthors\n")
          Log.info("Finished with histograms starting Authors")
          printOnce=true
        }

      val string:Array[String] = value.words.toString.split(',')

     val filtered:Array[String] = string.filter(x=>x.isEmpty==false)

      //stores all coauthors author had to find median max and minimum in form ,int,int,
      val int:Array[Int] = filtered.map(_.toInt)

      if(int.size == 0)
        {
          out.writeBytes(key.toString+","+value.toString+","+0+","+0+","+0+"\n")
        }
      else {
        scala.util.Sorting.quickSort(int)
        val max = int(int.size-1)
        val median = int(int.size/2)
        val mean:Double = int.sum.asInstanceOf[Double]/int.size.asInstanceOf[Double]
        val accents = Accents(key.toString)
        out.writeBytes(accents + "," + value.toString + "," + max.toString + "," + median.toString + "," + mean + "\n")
      }

    }

    override def close(context: TaskAttemptContext): Unit = {



      out.close()
    }
  }

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[Text, Author] = {

    val conf =  ConfigFactory.parseResources("config.conf")
    val file:Path = getDefaultWorkFile(job,conf.getString("name.extension"))
    val fs:FileSystem = file.getFileSystem(job.getConfiguration)
    val fileOut:FSDataOutputStream = fs.create(file,false)




    return new MyWriter(fileOut)
  }

  def Accents(string: String): String =
  {

    if(string.contains("&")) {
      var text =string
      text = text.replaceAll("&Ccedil;", "Ç")
      text = text.replaceAll("&ccedil;", "ç")
      text = text.replaceAll("&Aacute;", "Á")
      text = text.replaceAll("&Acirc;", "Â")
      text = text.replaceAll("&Atilde;", "Ã")
      text = text.replaceAll("&Eacute;", "É")
      text = text.replaceAll("&Ecirc;", "Ê")
      text = text.replaceAll("&Iacute;", "Í")
      text = text.replaceAll("&Ocirc;", "Ô")
      text = text.replaceAll("&Otilde;", "Õ")
      text = text.replaceAll("&Oacute;", "Ó")
      text = text.replaceAll("&Uacute;", "Ú")
      text = text.replaceAll("&aacute;", "á")
      text = text.replaceAll("&acirc;", "â")
      text = text.replaceAll("&atilde;", "ã")
      text = text.replaceAll("&eacute;", "é")
      text = text.replaceAll("&ecirc;", "ê")
      text = text.replaceAll("&iacute;", "í")
      text = text.replaceAll("&ocirc;", "ô")
      text = text.replaceAll("&otilde;", "õ")
      text = text.replaceAll("&oacute;", "ó")
      text = text.replaceAll("&uacute;", "ú")
      return text
    }
    return string

  }
}
