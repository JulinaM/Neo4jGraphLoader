package com.julina.graphDatabase

import org.apache.spark.sql.{Encoders, SparkSession}


object DriverApp {

    case class Tag(host: String, jobid: String)
    case class Log(aggregateTags: List[String], dps: Map[String, Float], metric: String, tags: Tag)

    def main(args: Array[String]): Unit = {
        System.out.println("--------------------------- STARTING  THE SPARK APPLICATION -------------------")
        var jsonFilePath = "/users/kent/jmaharja/opt/Projects/Neo4j_project/single.json"

        System.out.println("\n-------------------------- ARGUMENTS--------------------------------------------")
        args.foreach(println)

        if(args.length > 0) {
            jsonFilePath = args(0)
        }


        val schema = Encoders.product[Log].schema
        val spark = SparkSession.builder
                .appName("Neo4j LOADER")
                .config("spark.master", "local")
                .getOrCreate()


        System.out.println("\n --------------------------- Reading JSON File : %s", jsonFilePath)
        val dataFrame = spark.read.option("wholeFile", true).option("mode", "PERMISSIVE").option("multiline", "true").schema(schema).json(jsonFilePath)
        dataFrame.printSchema()
        dataFrame.createOrReplaceTempView("node")
        val df = spark.sqlContext.sql("select tags.host as host, tags.jobid as job, dps from node")

        System.out.println("-------------------------- WRITING TO NEO4J DATABASE----------------------------")
        val rdd = df.repartition(10).rdd
        val newrdd = rdd.mapPartitions(partition => {
                val newPartition = partition.map(x => {
                    DBHolder.prepareAndWrite(x)
                }).toList
                newPartition.iterator
            }).cache()
        System.out.println("-------------------------- Successful !! ------------------------------------------------------")
        System.out.println("-------------------------- Total "+ newrdd.count()+ " nodes written to NEO4J Database. --------- ")
        System.out.println("-------------------------- Shutting down APACHE SPARK !! ---------------------------------------")
        spark.close()

    }
}