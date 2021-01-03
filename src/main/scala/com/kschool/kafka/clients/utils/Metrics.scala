package com.kschool.kafka.clients.utils

import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.collection.JavaConverters._

object Metrics {
  private val statsMap: concurrent.Map[String, Long] = new ConcurrentHashMap[String, Long]().asScala

  def updateMetric(key: String): Unit = {
    val currentCount = statsMap.getOrElse(key, 0L)
    statsMap.put(key, currentCount + 1L)
  }

  def show(): Unit = {
    println("----- STATS ------")
    statsMap.toSeq.sortBy(_._1).foreach { case (metricName, metricValue) =>
      println(s"$metricName: $metricValue")
    }
    println("------------------")
  }

}
