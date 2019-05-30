package com.julina.graphDatabase

import org.apache.spark.sql.SparkSession
import org.neo4j.driver.v1.Values.parameters
import org.neo4j.driver.v1.{AuthTokens, Driver, GraphDatabase}
//import org.apache.spark.{ SparkConf, SparkContext}
import org.apache.spark.sql.Encoders


object DriverApp {

    case class Tag(host: String, jobid: String)

    case class Log(aggregateTags: List[String], dps: Map[String, String], metric: String, tags: Tag)

    object Holder {
        def neo4jDriver(): Driver = {
            GraphDatabase.driver("bolt://wasp.cs.kent.edu/7687", AuthTokens.basic("neo4j", "password"))
        }
    }


    def main(args: Array[String]): Unit = {
        val schema = Encoders.product[Log].schema
        val spark = SparkSession.builder
                .appName("Neo4j LOADER")
                .config("spark.master", "local")
                .getOrCreate()
        val dataFrame = spark.read.option("multiline", "true").schema(schema).json("/users/kent/jmaharja/opt/Projects/Neo4j_project/active_mem.json")
        System.out.println("------------------------------------------------------------Dhan Dhan Dhan --------------------------------------------------------------")
        dataFrame.printSchema()
        dataFrame.registerTempTable("node")
        val df = spark.sqlContext.sql("select tags.host as host, tags.jobid as job from node")
        System.out.println("------------------------------------------------------------AGAIN--------------------------------------------------------------")

//        val df100 = textFile.take(100)
//        df100.foreach(println(_))
//        df.foreach{x => writeToNeo4j(x(0).asInstanceOf[String].toString, x(1).asInstanceOf[String].toString)}

        System.out.println("------------------------------------------------------------WRITING TO NEO4J DATABASE--------------------------------------------------------------")
        args.foreach(println)
//        val neo4jDriver = GraphDatabase.driver("bolt://wasp.cs.kent.edu/7687", AuthTokens.basic("neo4j", "password"))
        df.foreach(x => writeToNeo4j(x(0).asInstanceOf[String].toString,
            x(1).asInstanceOf[String].toString))
    }

    def writeToNeo4j(host: String, job: String): Unit = {
        try {
            val neo4jSession = Holder.neo4jDriver().session()
            val tx = neo4jSession.beginTransaction()
            tx.run("MERGE(h:host {host_id: {host_id}})" +
                    "MERGE (j:job {job_id: {job_id}})" +
                    "MERGE (h)- [r:mem_usage]-(j)", parameters("host_id", host, "job_id", job))
            tx.success()// Mark this write as successful.

//            val result = neo4jSession.run("MATCH (a:Person) WHERE a.name STARTS WITH {x} RETURN a.name AS name", parameters("x", "J"))
//            // Each Cypher execution returns a stream of records.
//            while (result.hasNext) {
//                val record = result.next
//// Values can be extracted from a record by index or name.
//                System.out.println(record.get("name").asString)
//            }
            neo4jSession.close()
        }
        catch {
            case e: Exception => println("exception caught: " + e);
        }
        System.out.println("DONEEEEEEEEE")
        Holder.neo4jDriver().close()
    }


//
//    def write(log:Log) : Unit = {
//        println(log.tags.host+ " "+ log.tags.job)
//    }


//    def printOutput(uri: String, user: String, password: String): Unit = {
//        val neo4jDriver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
//        try{
//            val neo4jSession = neo4jDriver.session()
//            val tx = neo4jSession.beginTransaction()
//            tx.run("MERGE (a:Person {name: {x}})", parameters("x", name))
//            tx.success()// Mark this write as successful.
//        }
//    }

//    def write(jobid: String, host: String, neo: org.neo4j.spark.Neo4j): Unit = { neo.cypher(s"""
//            MERGE (h:host {host_id: $host})
//            MERGE (j:job {job_id: $jobid})
//            MERGE (h)-[r:mem_usage]-(j)"""
//        )
//    }

//
//      def test(args: Array[String]): Unit = {
//          val spark = SparkSession.builder
//                  .appName("Word Count")
//                  .config("spark.master", "local")
//                  .getOrCreate()
//        // Load the text into a Spark RDD, which is a distributed representation of each line of text
//        val textFile = spark.textFile("/home/jmaharja/IdeaProjects/Neo4jGraphLoader/src/main/test.txt")
//
//        //word count
//        val counts = textFile.flatMap(_.split(" "))
//          .map((_, 1))
//          .reduceByKey(_ + _)
//
//        counts.foreach(println)
//        System.out.println("Total words: " + counts.count());
//        counts.saveAsTextFile("/tmp/shakespeareWordCount")
//      }

}