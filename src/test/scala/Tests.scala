
import java.io.File

import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before

import scala.xml.Source



class Tests {

  val conf =  ConfigFactory.parseResources("config.conf")

  @Before def initialize(){

  }

  @Test def verifyConfExists()
  {
    assertFalse(conf.isEmpty)
  }

  @Test def checkBins(): Unit =
  {
    assertTrue(conf.getInt("bins.binRangesAuthor")>0)
    assertTrue(conf.getInt("bins.RangesYear")>0)
  }

  @Test def checkPath(): Unit ={
    assertTrue(conf.getBoolean("path.commandline")||(!conf.getString("path.in").isEmpty&&(!conf.getString("path.out").isEmpty)))
  }

  @Test def checkName():Unit ={

  assertTrue(conf.getString("name.job").isEmpty==false)
  }

  @Test def checkExtension(): Unit ={
    assertTrue(conf.getString("name.extension").startsWith("."))
  }

}
