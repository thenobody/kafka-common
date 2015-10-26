package com.rightster.data.redis

import java.util.UUID

import redis.clients.jedis.Jedis

/**
 * Created by antonvanco on 23/09/2015.
 */
object HLLMain {
  val percentFraction = 20

  def main(args: Array[String]): Unit = {
    val entryCount = if (args.nonEmpty) args(0).toInt
    else {
      print("Number of unique entries: ")
      io.StdIn.readInt()
    }
    println(s"Generating $entryCount entries")
    val set = scala.collection.mutable.Set.empty[String]

    val runtime = Runtime.getRuntime
    val startMemory = runtime.totalMemory - runtime.freeMemory

    (1 to percentFraction).foreach { i => if (i < percentFraction) print("--") else print("-|") }
    println()

    val jedis = new Jedis()
    val pipeline = jedis.pipelined()
    (1 to entryCount).foreach { i =>
      if (i % (entryCount / percentFraction) == 0) print(". ")
      val uuid = UUID.randomUUID().toString
      pipeline.pfadd("uuid-counter", uuid)
//      jedis.pfadd("uuid-counter", uuid)
      set.add(uuid)
    }
    println()
    pipeline.sync()

    println(s"final set size: ${set.size}")

    System.gc()
    Thread.sleep(1000)

    val endMemory = runtime.totalMemory - runtime.freeMemory

    val mb = 1024*1024
    println(s"MEMORY USAGE: ${(endMemory - startMemory)/mb} mb")
  }

}
