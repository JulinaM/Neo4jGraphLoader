package com.julina.graphDatabase

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator


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

        // val tags = StructType(StructField("host", StringType)::StructField("job", StringType)::Nil)
        val schema = StructType(StructField("metric", StringType) :: StructField("dps", StringType) :: StructField("tags", StringType) :: Nil)
        //        val schema = Encoders.product[Log].schema
        val spark = SparkSession.builder
                .appName("Neo4j LOADER")
//                .config("spark.master", "spark://jupiter:7077")
//                .config("spark.shuffle.service.enabled", "false")
//                .config("spark.dynamicAllocation.enabled", "false")
//                .config("spark.io.compression.codec", "snappy")
//                .config("spark.rdd.compress", "true")
                // .config("spark.cores.max", "16")
                .getOrCreate()


        System.out.println("\n --------------------------- Reading JSON File : %s", jsonFilePath)
        val dataFrame = spark.read.option("multiline", "true").schema(schema).json(jsonFilePath).as(Encoders.bean(Log.getClass)).persist(StorageLevel.MEMORY_ONLY_SER_2)
        dataFrame.cache()
        System.out.println("\n --------------------------- Estimation: %s", SizeEstimator.estimate(dataFrame))

        dataFrame.printSchema()
//        dataFrame.createOrReplaceTempView("node")
//        val df = spark.sqlContext.sql("select tags.host as host, tags.jobid as job, dps from node").persist(StorageLevel.MEMORY_ONLY_SER_2)
        System.out.println("-------------------------- WRITING TO NEO4J DATABASE----------------------------")
//        val rdd = df.repartition(16).rdd.persist(StorageLevel.MEMORY_ONLY_SER_2)
//        dataFrame.toDF().take(10000).foreach(t=>println(t(0)+ "\t"+t(1))+ "\t" + t(2))

//        dataFrame.toDF().take(10000).foreach(DBHolder.prepareAndWrite(_))//TODO
        dataFrame.toDF().collect().foreach(DBHolder.prepareAndWrite(_))
        /*val newrdd = dataFrame.toDF().repartition(64).rdd.mapPartitions(partition => {
                 val newPartition = partition.map(x => {
                     DBHolder.prepareAndWrite(x)
                 }).toList
                 newPartition.iterator
             }).persist(StorageLevel.MEMORY_ONLY_SER_2)*/
//        System.out.println("\n --------------------------- Estimation: %s", SizeEstimator.estimate(newrdd))
        System.out.println("-------------------------- Successful !! ------------------------------------------------------")
//        System.out.println("-------------------------- Total "+ newrdd.count()+ " nodes written to NEO4J Database. --------- ")
        System.out.println("-------------------------- Shutting down APACHE SPARK !! ---------------------------------------")
        spark.close()
        spark.stop()

    }
}