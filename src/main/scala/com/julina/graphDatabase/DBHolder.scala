package com.julina.graphDatabase
import org.apache.spark.sql.Row
import org.json4s.jackson.JsonMethods.parse
import org.neo4j.driver.v1.Values.parameters
import org.neo4j.driver.v1._


object DBHolder{
    val driver: Driver = GraphDatabase.driver("bolt://wasp.cs.kent.edu/7687", AuthTokens.basic("neo4j", "password"))

    def write(query: String, parameters: Value): Unit = {
        try {
            val session = driver.session
            try
                session.writeTransaction(new TransactionWork[Integer]() {
                    @Override
                    def execute(tx: Transaction): Integer = {
                        tx.run(query, parameters)
                        tx.success()
                        1
                    }
                })
            finally if (session != null) session.close()
        }
    }

    def close(): Unit = {
        driver.close()
    }

    def prepareAndWrite(row: Row): Unit = {
        val tagMap: Map[String, Int] = parse(row(2).asInstanceOf[String]).values.asInstanceOf[Map[String, Int]]
        //tagMap.foreach{ case (key: String, value: String) => println(">>> key=" + key + ", value=" + value)}
        val host = tagMap.getOrElse("host", "null_host").asInstanceOf[String]
        val job = "job_" + tagMap.getOrElse("jobid", 0).asInstanceOf[String]
        println(host + "\t" + job)

        val dpsMap: Map[String, Double] = parse(row(1).asInstanceOf[String]).values.asInstanceOf[Map[String, Double]]
        //dpsMap.foreach{ case (key: String, value: Any) => println(">>> key=" + key + ", value=" + value)}
        val relation: String = dpsMap.map(_.productIterator.mkString(":")).mkString(",")
        println(relation)

        if (relation == null || relation.isEmpty) {
            val query = "MERGE (h:host {host_id: {host_id}}) - [r:relation] - (j:job {job_id: {job_id}})"
            write(query, parameters("host_id", host, "job_id", job))
        }
        else {
            val query = "MERGE (h:host {host_id: {host_id}}) - [r:relation] - (j:job {job_id: {job_id}}) " +
                    "with reduce(result={relation_val}, e in collect(r.mem_usage) | result + ','+ e ) as new_relation, r,h,j " +
                    "MERGE (h)-[r2:relation]-(j) " +
                    "SET r2.mem_usage = new_relation "
            write(query, parameters("host_id", host, "job_id", job, "relation_val", relation))
        }

        /* var counter: Int = 0
         for ((k, v) <- dpsMap) {
             val relationKey = "id_" + counter
             System.out.println("""relation_id: %s  | timestamp: %s, memory: %s """.format(relationKey, k, v ))
             val newQuery = "MATCH (h:host {host_id: {host_id} })-[r:mem_usage]-(j: job {job_id: {job_id} })" +
                     " SET r.%s = '[timestamp = %s , memory = %s]' return r".format(relationKey, k, v.toString)
             write(newQuery, parameters("host_id", host, "job_id", job))
             counter = counter + 1
         }*/
    }

}


