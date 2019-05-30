package com.julina.graphDatabase

import org.apache.spark.sql.{Encoders, SparkSession}


object DriverApp {

    case class Tag(host: String, jobid: String)
    case class Log(aggregateTags: List[String], dps: Map[String, String], metric: String, tags: Tag)

    def main(args: Array[String]): Unit = {
        System.out.println("--------------------------- STARTING  THE SPARK APPLICATION -------------------")

        System.out.println("\n-------------------------- ARGUMENTS--------------------------------------------")
        args.foreach(println)

        val schema = Encoders.product[Log].schema
        val spark = SparkSession.builder
                .appName("Neo4j LOADER")
                .config("spark.master", "local")
                .getOrCreate()


        val dataFrame = spark.read.option("multiline", "true").schema(schema).json("/users/kent/jmaharja/opt/Projects/Neo4j_project/active_mem.json")
        dataFrame.printSchema()
        dataFrame.createOrReplaceTempView("node")
        val df = spark.sqlContext.sql("select tags.host as host, tags.jobid as job from node")

        System.out.println("-------------------------- WRITING TO NEO4J DATABASE----------------------------")
        val rdd = df.repartition(10)
            rdd.mapPartitions(partition => {
                val newPartition = partition.map(x => {
                    DBHolder.prepareAndWrite(x)
                }).toList
                DBHolder.close()
                newPartition.iterator
            })

    }
}