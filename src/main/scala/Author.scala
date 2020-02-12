import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text, Writable}

import scala.collection.immutable.TreeMap

class Author(authorShip:DoubleWritable,Count: IntWritable,text: Text) extends Writable{
val AuthorShip:DoubleWritable = authorShip//score of author
val count:IntWritable = Count//current total number of appearances in the data base
val words:Text = text//stores anything that would be needed later such as a list of coauthors in int,int format to be split later

  override def write(dataOutput: DataOutput): Unit ={
    AuthorShip.write(dataOutput)
    count.write(dataOutput)
    words.write(dataOutput)
  }
  override def readFields(dataInput: DataInput): Unit =
  {
    AuthorShip.readFields(dataInput)
    count.readFields(dataInput)
    words.readFields(dataInput)

  }

  //required empty constructor
  def this() = this(authorShip = new DoubleWritable(0.0),Count = new IntWritable(0),new Text("") )


  //adds a double to current authorship
  def updateAuthorShip(value:Double): Unit={
    val newScore:Double = AuthorShip.get()+value
    AuthorShip.set(newScore)
  }

  //updates count of current author
  def updateCount(value:Int):Unit={
    val newScore:Int = count.get()+value
    count.set(newScore)
  }

  def getAuthorship :Double =
    {
      return AuthorShip.get()
    }
  def getCount:Int =
  {
    return count.get()
  }

  override def toString: String = {
    return ( AuthorShip.toString + "," + count.toString)

  }
//update words mostly designed to store histogram but also stores number of coAuthors for each author
  def updateWords(string: String):Unit ={
    if(string.isEmpty)
      return

    val strings =words.toString+string+","
    words.set(strings)

  }

  //testing ignore
  def createbinYear(): Unit =
  {
  }

  def updateMax(int:Int): Unit =
  {
    val x = words.toString.split(':')
   if(x(0)!=null)
     {
       if(x(0).trim.toInt>int)
         return
       else
         {
           words.set(words.toString.replace(x(0),int.toString))
         }
     }


  }

  def updateMean(int:Int,double: Double): Unit =
  {
    val x = words.toString.split(':')
    val mean = x(1).toDouble
    val newCount = int + count.get()

    val newMean:Double =((double*int.asInstanceOf[Double])+(mean*count.get.asInstanceOf[Double]))/newCount.asInstanceOf[Double]
    words.set(words.toString.replace(":"+x(1),":"+newMean))

  }


}