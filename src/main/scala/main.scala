
import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, LongWritable, Text, Writable}
import org.apache.hadoop.mapreduce.{InputSplit, Job, Mapper, RecordReader, Reducer, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.TreeMap

object main {

  val conf = ConfigFactory.parseResources("config.conf")
  val binRangesYear = conf.getInt("bins.binRangesYear")
  val binRangesAuthor = conf.getInt("bins.binRangesAuthor")
  class Counter extends Mapper[Text, Text, Text, Author] {

    override def cleanup(context: Mapper[Text, Text, Text, Author]#Context): Unit = {
      if(context.getProgress>.99)
        {
          context.write(new Text("#"),new Author(new DoubleWritable(0.0),new IntWritable(0),new Text("")))
        }
    }
    override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Author]#Context): Unit = {

      val input: Array[String] = value.toString.split(',')
      val typeOfEntry: String = input(0)

      val AuthorArray: Array[String] = input(1).split('*')


      context.write(new Text("\""+typeOfEntry.trim),new Author(new DoubleWritable(0.0),new IntWritable(1),new Text(AuthorArray.size.toString+":"+AuthorArray.size.toString)))
      context.write(new Text("\"all"),new Author(new DoubleWritable(0.0),new IntWritable(1),new Text(AuthorArray.size.toString+":"+AuthorArray.size.toString)))
      //histogram keys print order follows ascii order that gets written here so thats why their values are strange
      //as we went them to print to the top
      if(input.length==3&&(!input(2).isEmpty)&&(!input(2).contains("null"))) {
        val year:Int =(input(2).toInt/binRangesYear)*binRangesYear
        val x = new Author(new DoubleWritable(), new IntWritable(), new Text(","+year.toString + ":" + AuthorArray.size.toString))
        context.write(new Text("\"A"),x )//maps year bins to the top of the list of keys so it prints first
      }
      val x = AuthorArray.length/binRangesAuthor*binRangesAuthor
      context.write(new Text("\"D"),new Author(new DoubleWritable(), new IntWritable(),new Text(","+x.toString+":1")))
      if(key.toString.contains("key=\"journals"))//Maps journals to be printed to histogram
        {
          context.write(new Text("\"B"),new Author(new DoubleWritable(), new IntWritable(),new Text(","+x.toString+":1")))
        }
      if(key.toString.contains("key=\"conf"))//Maps conferences to be printed to histogram
        {
          context.write(new Text("\"C"),new Author(new DoubleWritable(), new IntWritable(),new Text(","+x.toString+":1")))
        }



      //writes each author to the map to be sent to the reducer
      var counter:Int =0
      AuthorArray.foreach(author => {
        if (AuthorArray.length == 1)
          context.write(new Text(author), new Author(new DoubleWritable(1.0), new IntWritable(1),new Text((AuthorArray.length).toString)))
        else {
          if (counter == 0) {
            context.write(new Text(author), new Author(new DoubleWritable(1.0/AuthorArray.length + (1.0 / (4.0 * AuthorArray.length))), new IntWritable(1),new Text((AuthorArray.size).toString)))
            counter+=1
          }
          else if (counter == AuthorArray.length - 1) {
            context.write(new Text(author), new Author(new DoubleWritable(1.0/AuthorArray.length - (1.0 / (4.0 * AuthorArray.length))), new IntWritable(1),new Text((AuthorArray.size).toString)))
            counter +=1
          }
          else {
            context.write(new Text(author), new Author(new DoubleWritable(1.0 / AuthorArray.length), new IntWritable(1),new Text((AuthorArray.length).toString)))
            counter+=1
          }
        }

      })
    }

  }

  var tracker: TreeMap[Double, String] = new TreeMap[Double, String]

    class inputFormat extends FileInputFormat[Text, Text] {
      override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, Text] = {

      val authorRead: AuthorRecordReader = new AuthorRecordReader()
      authorRead.initialize(split, context)
      return authorRead
    }
    }

    def main(args: Array[String]): Unit = {


      val job = Job.getInstance(new Configuration, conf.getString("name.job"))

    if (args.size < 2&& conf.getBoolean("path.commandline")==true) {
      return
      }
      else if(args.size>1&&conf.getBoolean("path.commandline")==true)
      {
        FileInputFormat.addInputPath(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))
      }
      else if(conf.getBoolean("path.commandline")==false)
      {
        FileInputFormat.addInputPath(job, new Path(conf.getString("path.in")))
        FileOutputFormat.setOutputPath(job, new Path(conf.getString("path.out")))
      }




      job.setJarByClass(this.getClass)
      job.setMapperClass(classOf[Counter])
      job.setInputFormatClass(classOf[inputFormat])

      job.setReducerClass(classOf[authorReducer])
      job.setCombinerClass(classOf[authorReducer])

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Author])
      job.setOutputFormatClass(classOf[CustomOutput])


      job.waitForCompletion(true)


    }
  }