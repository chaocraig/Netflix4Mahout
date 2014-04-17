/*
 * Author:  Craig Kuo-Jen Chao
 * Date: 2014/04/17
 * Topic: Translate the Netflix Dataset into Mahout CF input dataset
 * Description: 
 *     Nextfile dataset format: 
 *          A. format of file names: 
 */


package com.vpon


import java.io._
import java.io.IOException
import java.net.DatagramSocket
import java.net.ServerSocket
import java.util.Random
import java.util.regex._
import java.nio.file.{Files => Files, Path}

//import scala.collection.parallel.Foreach
//import scala.collection.JavaConversions._
//import org.apache.spark._
//import org.apache.spark.SparkContext._
//import org.apache.spark.rdd.{RDD => SparkRDD}


object TranslateFiles {
  private val MINPORT     = 10000
  private val MAXPORT     = 49151
  private val MAXTRY      = 100
  private val DEBUG_LEVEL = 1
  private val DATE_ERR    = "2000-00-00"

  //private val SRC_FILEPATH="hdfs://hadoop-001:9000/user/cray/app_dect/imei_all.txt"
  //private val DST_FILEPATH="hdfs://hadoop-001:9000/user/cray/app_dect/imei_all_dir"

  //val SRC_FILEPATH="/Users/cray/Downloads/Netflix/download/training_set/mv_0000001.txt"
  val SRC_FILEPATH="/Users/cray/Downloads/Netflix/download/training_set/"
  val DST_FILEPATH="/Users/cray/Downloads/Netflix/netflix4mahout.txt"

  

  //def ReadAllFromFile(sc:SparkContext, filename:String) : Iterator[String] =  {
  def ReadAllFromFile(filename:String) : List[String]  =  {
  //def ReadAllFromFile(file:File) : List[String]  =  {
    
    
    //get whole file content
    //val listLines = scala.io.Source.fromFile(filename, "utf-8").getLines.toList
    val listLines = scala.io.Source.fromFile(filename, "utf-8").getLines.toList
 
    
    if (DEBUG_LEVEL > 1) println ("---> ReadAllFromFile() finished.")
    if (DEBUG_LEVEL > 2) println ("---> ReadAllFromFile() Count: " + listLines.length + "\n" )

    listLines.toList
  }
  
 

  def DoConversions(listLines:List[String]) : List[String] = {   

    if (DEBUG_LEVEL > 2) println ("---> Enter DoConversions() Count: " + listLines.length + "\n" )

    var listConverted =  List[String](null)
    var strItem = ""
    
    for (line <- listLines) {
       if (line.last == ':') {
           strItem = line.substring(0, line.length()-1) //remove the tailed ":"
           //println ("Enter strIem check..." + line + "["+strItem+"]" )
       } else {
           val ptnUserRateDate = "(\\d+),(\\d+),(\\d+-\\d+-\\d+)".r
           line match {
               case ptnUserRateDate(strUser, strRate, strDate) => {                
                    listConverted ::= strItem +',' +strUser +',' +strRate +'\n'
               }
               case _ =>  if (DEBUG_LEVEL > 2) println ("Convert Err: " + line + '\n')
           }           
       }       
    } 

    if (DEBUG_LEVEL > 1) println ("---> DoConversions() finished.")
    if (DEBUG_LEVEL > 2) println ("---> Leave DoConversions() Count: " + listConverted.length + "\n" )
    if (DEBUG_LEVEL > 5) listConverted.foreach(println (_))

    listConverted
  }
  
  
  def CreateOutFile(path:String) :FileWriter = {
      
      var fw: FileWriter = null

      try {
          fw = new FileWriter(path) 

      } catch  {
          case ex: IOException => println("IO Exception")          
          case _ => println( "===> Error: Create file [" + path + "]" )    
      } finally {
    	  if (DEBUG_LEVEL > 1) println ("---> Create File("+ path +") finished.")
      }      
      fw
  }
   
  def WriteAlltoFile(listLines:List[String], fw:FileWriter)  {
      

      try {
          //val fw = new FileWriter(path) 
          fw.write(listLines.mkString) 

      } catch  {
          case ex: IOException => println("IO Exception")          
          case _ => println( "===> Error: Write file!" )    

      } finally {
    	  if (DEBUG_LEVEL > 1) println ("---> WriteAlltoFile() finished.")
    	  if (DEBUG_LEVEL > 2) println ("---> WriteAlltoFile() Count: " + listLines.length + "\n" + listLines)
    	  if (DEBUG_LEVEL > 5) listLines.foreach(println (_)) 
      }
      
  }
  
  
    def findAvailablePort: Int = {
    var ss: ServerSocket = null
    var ds: DatagramSocket = null
    var port: Int = 0
    val rand = new Random()

    1 to MAXTRY find {i =>
      try {
        val tryport = MINPORT + rand.nextInt(MAXPORT-MINPORT)
        ss = new ServerSocket(tryport)
        ss.setReuseAddress(true)

        ds = new DatagramSocket(tryport)
        ds.setReuseAddress(true)

        port = tryport
        true
      } catch {
        case e: Exception => false
      } finally {
        if (ss != null) ss.close
        if (ds != null) ds.close
      }
    }

    port
  }

/*
  def findJars: Seq[String] = {
    val jarOfClass = SparkContext.jarOfClass(this.getClass)
    val version = scala.util.Properties.versionNumberString
      .split('.').init.mkString(".")
    // no jar found, assume running from sbt, try to add jars
    if (jarOfClass.length == 0) {
      new java.io.File("target/scala-" + version)
        .listFiles.map(_.getPath).filter(_.endsWith(".jar"))
    } else
      jarOfClass
  }


  
  def setSparkEnv(master:String) : SparkContext = {

    val conf = new SparkConf()
       .setMaster(master)
       .setAppName("mllib tutorial")
       // runtime Spark Home, set by env SPARK_HOME or explicitly as below
       //.setSparkHome("/opt/spark")

       // be nice or nasty to others (per node)
       .set("spark.executor.memory", "4g")
       //.set("spark.core.max", "8")

       // find a random port for driver application web-ui
       .set("spark.ui.port", findAvailablePort.toString)
       .setJars(findJars)
       
       // The coarse-grained mode will instead launch only one long-running Spark task on each Mesos machine, 
       // and dynamically schedule its own mini-tasks within it. The benefit is much lower startup overhead, 
       // but at the cost of reserving the Mesos resources for the complete duration of the application.
       // .set("spark.mesos.coarse", "true")

    // for debug purpose
    println("sparkconf: " + conf.toDebugString)

    val sc = new SparkContext(conf)
    sc
  }

*/
    
    
  def ProcessFile(srcFilePath:String, outFileHandle:FileWriter) {
       //for(file <- stringSrcDir.listFiles if file.getName endsWith ".txt") {
       //WriteAlltoFile(DoConversions(ReadAllFromFile(sc, SRC_FILEPATH)), DST_FILEPATH)
      var listLines : List[String] = (null)
      if (srcFilePath.endsWith(".txt")) {
          listLines = ReadAllFromFile(srcFilePath)
      }
    
      if (DEBUG_LEVEL > 0) println ("---> Processing file ["+srcFilePath+"] ...")
      //println ("listLines == " + listLines.length)
    
      val listConverted = DoConversions(listLines)
      WriteAlltoFile(listConverted, outFileHandle)           
  }

  
  def main(args:Array[String]){
     var intFileCount = 0
     var fw:FileWriter = null
     //val sc = setSparkEnv( args(0) )
     
     if ( (fw=CreateOutFile(DST_FILEPATH)) == null) return //error
       
    
     try {
         val files = new File(SRC_FILEPATH).listFiles.toList.foreach(
    		 f => {
    		    ProcessFile(f.toString, fw)
    		    intFileCount += 1
    		 }
         )
     } catch {
          case ex: IOException => println("IO Exception")          
          case _ => println( "===> Error: Read directory [" + SRC_FILEPATH + "]" )    

      } finally {
    	  if (DEBUG_LEVEL > 1) println ("---> Read directory finished." + SRC_FILEPATH)
    	  if (DEBUG_LEVEL > 0) println ("---> Total files# " + intFileCount + "\n" )
      }
           
      fw.close()   	
  }
}
  
