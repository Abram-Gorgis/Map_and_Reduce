import java.lang

import main.tracker
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex

class authorReducer extends Reducer[Text, Author, Text, Author] {
  val Log:Logger = LoggerFactory.getLogger(classOf[authorReducer])

  override def reduce(key: Text, values: lang.Iterable[Author], context: Reducer[Text, Author, Text, Author]#Context): Unit = {

    val ReducedAuthor: Author = new Author(new DoubleWritable(0.0), new IntWritable(0), new Text(""))
    if(key.toString.contains("#")) {
      context.write(key, ReducedAuthor)
      return
    }
    values.forEach(author => {
      ReducedAuthor.updateAuthorShip(author.getAuthorship)
      //Yet again weird histogram keys so they will print to the top all are built the same even though the track different statistics they will all also
      //print the same in custom output
      if ((key.toString.contains("\"A")||key.toString.contains("\"D")||key.toString.contains("\"B")||key.toString.contains("\"C")) && !ReducedAuthor.words.toString.isEmpty) {
        Log.info("Starting histogram for key: "+key.toString)
        val x: Array[String] = author.words.toString.split(',').filter(x=>(!x.isEmpty))
        val y:String= author.words.toString
        var string: String = ReducedAuthor.words.toString
        x.foreach(v1=>{
          if(!v1.isEmpty) {
            val v: String = v1
            val z = v.split(":")
            if (string.contains(","+z(0) + ":") && z(1) != null) {
              val numberS = StringUtils.substringBetween(string, ( ","+z(0) + ":"), ",")
              val number = (numberS.toInt + (z(1).toInt))
              string = StringUtils.replace(string,","+z(0) + ":" + numberS+",",","+z(0) + ":" + number.toString+",")
              ReducedAuthor.words.set(string)
            }
            else
              ReducedAuthor.updateWords(v1)
          }
          else
            Log.error("Skipped over value: "+v1+" in historgram for key")
        })

      }
      else if(checker(key))
        {

          if(ReducedAuthor.words.toString.isEmpty)
          ReducedAuthor.words.set("0:0")

          val x = author.words.toString.split(':')
          ReducedAuthor.updateMax(x(0).toInt)
          ReducedAuthor.updateMean(author.count.get(),x(1).toDouble)

        }
      else
        ReducedAuthor.updateWords(author.words.toString)

      ReducedAuthor.updateCount(author.getCount)
    })


    if(!key.toString.contains("\""))
    tracker += (ReducedAuthor.getAuthorship -> key.toString)


      context.write(key, ReducedAuthor)

  }
  def checker(key: Text): Boolean = {//checks for these tags for better printing later on
    val string = key.toString
    if(string.contains("\"book")||string.contains("\"article")||string.contains("\"inproceedings")||string.contains("\"proceedings")||string.contains("\"incollection")
    ||string.contains("\"phdthesis")||string.contains("\"mastersthesis")||string.contains("\"www")||string.contains("\"all"))
      return true

    return false
  }

  override def cleanup(context: Reducer[Text, Author, Text, Author]#Context): Unit = {
    Log.info("Cleanup called in reducer with progress: "+context.getProgress)

    if(context.getProgress>.98)
      {
        val y = tracker.take(100)
        val z = tracker.takeRight(100)
        val x = new Author(new DoubleWritable(0.0),new IntWritable(0),new Text("Bottom 100 ascending:"))
        y.foreach(author=>x.words.set(x.words.toString+","+author._2+":"+(author._1.toString)))
        x.words.set(x.words.toString+"\nTop 100 descending,")
        var string:String= ""
        z.foreach(author=>string =author._2+":"+(author._1.toString)+","+string)
        x.words.set(x.words.toString+string+"\n")
        context.write(new Text("#"),x)
      }


  }
}