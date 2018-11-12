package com.madhouse.dsp

import org.apache.commons.cli._
import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{when, upper, split}

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

object Cassandra {

  case class R(did: String, os: Int, tag: Seq[String])

  def main(args: Array[String]): Unit = {

    var seed = "127.0.0.1"
    var table = ""
    var keyspace = ""
    var file = ""
    var master = ""
    var json = false
    var cluster = ""
    var apiType = "dataframe"
    var operation = ""

    val opt = new Options()
    opt.addOption("a", "api-type", true, "spark api type: rdd or dataframe")
    opt.addOption("c", "cluster", true, "cluster name of cassandra")
    opt.addOption("s", "seed", true, "seeds of cassandra")
    opt.addOption("t", "table", true, "table name in cassandra")
    opt.addOption("k", "keyspace", true, "keyspace name in cassandra")
    opt.addOption("f", "file", true, "read file path in hdfs")
    opt.addOption("j", "json", false, "the file is json file")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("o", "operation", true, "operation for spark rdd: append,add,remove,overwrite")
    opt.addOption("m", "master", true, "master of spark")

    val formatstr = "sh run.sh yarn-cluster|yarn-client|local ...."
    val formatter = new HelpFormatter
    val parser = new PosixParser

    var cl: CommandLine = null
    try
      cl = parser.parse(opt, args)

    catch {
      case e: ParseException =>
        e.printStackTrace()
        formatter.printHelp(formatstr, opt)
        System.exit(1)
    }
    if (cl.hasOption("a")) apiType = cl.getOptionValue("a")
    if (cl.hasOption("c")) cluster = cl.getOptionValue("c")
    if (cl.hasOption("s")) seed = cl.getOptionValue("s")
    if (cl.hasOption("t")) table = cl.getOptionValue("t")
    if (cl.hasOption("k")) keyspace = cl.getOptionValue("k")
    if (cl.hasOption("f")) file = cl.getOptionValue("f")
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("j")) json = true
    if (cl.hasOption("o")) operation = cl.getOptionValue("o")
    if (cl.hasOption("m")) master = cl.getOptionValue("m")

    println(s"spark master = $master, cassandra cluser = $cluster, seed = $seed, " +
      s"keyspace = $keyspace, table = $table, hdfs file path = $file, file is json format: $json, " +
      s"spark api type = $apiType, spark rdd opertion = $operation")

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", seed)
    val sc = new SparkContext("spark://" + master + ":7077", "sparkCassandra", conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val start = System.currentTimeMillis()
    val df =
      if (json) sqlContext.read.json(file)
      else sqlContext.read.parquet(file)
    val mdf = df.select(when('did.contains("-"), upper('did)).otherwise('did) as "did", split('tags, ",") as "tag", 'os as "os").cache
    mdf.show()

    println(s"##### mdf has ${mdf.count()} records...")

    /*val rdd = sc.cassandraTable(keyspace, table)
    rdd.take(3).foreach(println)

    val col = sc.parallelize(Seq(("of", 1200), ("the", 863)))
    col.saveToCassandra(keyspace, table)*/

    if (apiType.equalsIgnoreCase("dataframe")) {
      mdf.write
        .cassandraFormat(table, keyspace, cluster)
        .mode("append")
        .save()
      /*mdf.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace, "cluster" -> cluster))
      .mode("append")
      .save()*/
    } else {
      operation.toLowerCase match {
        case "append" | "add" => mdf.map(r => R(r.getString(0), r.getLong(2).toInt, r.getSeq[String](1)))
          .saveToCassandra(keyspace, table, SomeColumns("did", "os", "tag" append))
        case "remove" => mdf.map(r => R(r.getString(0), r.getLong(2).toInt, r.getSeq[String](1)))
          .saveToCassandra(keyspace, table, SomeColumns("did", "os", "tag" remove))
        case "overwrite" => mdf.map(r => R(r.getString(0), r.getLong(2).toInt, r.getSeq[String](1)))
          .saveToCassandra(keyspace, table, SomeColumns("did", "os", "tag" overwrite))
      }
    }

    mdf.unpersist()
    println(s"#####===finished, use time: ${(System.currentTimeMillis() - start) / 1000}s!")
    sc.stop()
  }
}
