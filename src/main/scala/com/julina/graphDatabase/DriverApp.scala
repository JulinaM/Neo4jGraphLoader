package com.julina.graphDatabase

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator


object DriverApp {

    case class Tag(host: String, jobid: String)
    case class Log(aggregateTags: List[String], dps: Map[String, Float], metric: String, tags: Tag)

    def main(args: Array[String]): Unit = {
        System.out.println("\n --------------------------- STARTING  THE SPARK APPLICATION -------------------")
        var jsonFilePath = "/users/kent/jmaharja/opt/Projects/Neo4j_project/single.json"

        System.out.println("\n -------------------------- ARGUMENTS--------------------------------------------")
        args.foreach(println)

        if(args.length > 0) {
            jsonFilePath = args(0)
        }

        // val tags = StructType(StructField("host", StringType)::StructField("job", StringType)::Nil)
        val schema = StructType(StructField("metric", StringType) :: StructField("dps", StringType) :: StructField("tags", StringType) :: Nil)
        val spark = SparkSession.builder
                .appName("Neo4j LOADER")
                //.config("spark.master", "spark://jupiter:7077")
                .getOrCreate()


        System.out.println("\n --------------------------- Reading JSON File : %s", jsonFilePath)
        val dataFrame = spark.read.option("multiline", "true").schema(schema).json(jsonFilePath).as(Encoders.bean(Log.getClass)).persist(StorageLevel.MEMORY_ONLY_SER_2)
        dataFrame.cache()
        System.out.println("\n --------------------------- Estimation: %s", SizeEstimator.estimate(dataFrame))

        dataFrame.printSchema()
        System.out.println("\n -------------------------- WRITING TO NEO4J DATABASE----------------------------")
//                dataFrame.toDF().take(1000).foreach(DBHolder.prepareAndWrite(_))
        dataFrame.toDF().rdd.repartition(16).foreachPartition(partition => {
            val newPartition = partition.map(x => {
                DBHolder.prepareAndWrite(x)
            }).toList
            newPartition.iterator
        })

        System.out.println("\n -------------------------- Successful !! ------------------------------------------------------")
//        System.out.println("\n -------------------------- Total "+ newrdd.count()+ " nodes written to NEO4J Database. --------- ")
        System.out.println("\n -------------------------- Shutting down APACHE SPARK !! ---------------------------------------")
        spark.close()
        spark.stop()

    }
}