package com.julina.graphDatabase
import org.apache.spark.sql.Row
import org.neo4j.driver.v1.Values.parameters
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Session, Transaction, TransactionWork, Value}


object DBHolder{
    val driver = GraphDatabase.driver("bolt://wasp.cs.kent.edu/7687", AuthTokens.basic("neo4j", "password"))

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
        val query = "MERGE(h:host {host_id: {host_id}})" +
                "MERGE (j:job {job_id: {job_id}})" +
                "MERGE (h)- [r:mem_usage]-(j)"
        val job_id = "job_"+ row(1).asInstanceOf[String]
        //System.out.println(job_id)
        write(query, parameters("host_id", row(0).asInstanceOf[String], "job_id", job_id))

        val dps: Map[String, Float] = row(2).asInstanceOf[Map[String, Float]]
        System.out.println("DPS EMPTY:: "+dps.empty)
        var counter: Int = 0
        for ((k,v) <- dps) {
            val relationKey = "id_" + counter
            System.out.println("""relation_id: %s  | timestamp: %s, memory: %s """.format(relationKey, k, v ))
            val newQuery = "MATCH (h:host {host_id: {host_id} })-[r:mem_usage]-(j: job {job_id: {job_id} })" +
                    " SET r.%s = '[timestamp = %s , memory = %s]' return r".format(relationKey, k, v.toString)
            write(newQuery,  parameters("host_id", row(0).asInstanceOf[String], "job_id", job_id ))
            counter = counter + 1
        }

    }

    @Deprecated
    def writeToNeo4j(host: String, job: String, neo4jSession: Session) = {
        try {
            val tx = neo4jSession.beginTransaction()
            tx.run("MERGE(h:host {host_id: {host_id}})" +
                    "MERGE (j:job {job_id: {job_id}})" +
                    "MERGE (h)- [r:mem_usage]-(j)", parameters("host_id", host, "job_id", job))
            tx.success()// Mark this write as successful.
        }
        catch {
            case e: Exception => println("exception caught: " + e);
        }
        System.out.println("DONE")
    }
}


