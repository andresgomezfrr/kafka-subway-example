package com.kschool.kafka.exercise.utils

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.concurrent

object MapCache {
  private val cache: concurrent.Map[String, Long] = new ConcurrentHashMap[String, Long]().asScala

  def addUser(userId: String, lastSeen: Long): Unit = {
    Metrics.updateMetric("cache-add")
    cache.put(userId, lastSeen)
  }

  def removeUser(userId: String): Unit = {
    Metrics.updateMetric("cache-remove")
    cache.remove(userId)
  }

  def getUser(userId: String): Option[Long] = {
    Metrics.updateMetric("cache-get")
    cache.get(userId)
  }

}
