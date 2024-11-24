// Databricks notebook source
val archivo: String ="dbfs:/FileStore/lol_ranked_games.csv"

// COMMAND ----------

val df = spark.read.option("header", "true").option("inferSchema", "true").csv(archivo)

// COMMAND ----------

display(
  df
)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val firstTowerImpact = data.groupBy("isFirstTower")
  .agg(avg("hasWon").alias("winRate"))
firstTowerImpact.show()


// COMMAND ----------

import org.apache.spark.sql.functions._

val firstTowerImpact = data.groupBy("isFirstTower")
  .agg(avg("hasWon").alias("winRate"))
firstTowerImpact.show()


// COMMAND ----------

import org.apache.spark.sql.SparkSession


// COMMAND ----------

val spark = SparkSession.builder()
  .appName("LeagueOfLegendsAnalysis")
  .getOrCreate()

// COMMAND ----------

val filePath = "dbfs:/FileStore/lol_ranked_games.csv" 
val data = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

// COMMAND ----------

import org.apache.spark.sql.functions._

val firstTowerImpact = data.groupBy("isFirstTower")
  .agg(avg("hasWon").alias("winRate"))
firstTowerImpact.show()


// COMMAND ----------

val laneTurretImpact = data.select(
  (col("destroyedTopOuterTurret") + col("destroyedTopInnerTurret") + col("destroyedTopBaseTurret")).alias("topLaneTurrets"),
  (col("destroyedMidOuterTurret") + col("destroyedMidInnerTurret") + col("destroyedMidBaseTurret")).alias("midLaneTurrets"),
  (col("destroyedBotOuterTurret") + col("destroyedBotInnerTurret") + col("destroyedBotBaseTurret")).alias("botLaneTurrets"),
  col("hasWon")
).groupBy()
 .agg(
   avg("topLaneTurrets").alias("avgTopLaneImpact"),
   avg("midLaneTurrets").alias("avgMidLaneImpact"),
   avg("botLaneTurrets").alias("avgBotLaneImpact")
 )
laneTurretImpact.show()


// COMMAND ----------

val goldDiffByFrame = data.groupBy("frame")
  .agg(avg("goldDiff").alias("avgGoldDiff"))
  .orderBy("frame")
goldDiffByFrame.show()


// COMMAND ----------

val dragonImpact = data.select(
  col("killedFireDrake").alias("fireDrake"),
  col("killedWaterDrake").alias("waterDrake"),
  col("killedAirDrake").alias("airDrake"),
  col("killedEarthDrake").alias("earthDrake"),
  col("killedElderDrake").alias("elderDrake"),
  col("hasWon")
).groupBy()
 .agg(
   avg("fireDrake").alias("avgFireImpact"),
   avg("waterDrake").alias("avgWaterImpact"),
   avg("airDrake").alias("avgAirImpact"),
   avg("earthDrake").alias("avgEarthImpact"),
   avg("elderDrake").alias("avgElderImpact")
 )
dragonImpact.show()


// COMMAND ----------

val deathsImpact = data.groupBy("hasWon")
  .agg(avg("deaths").alias("avgDeaths"))
deathsImpact.show()


// COMMAND ----------

val baronImpact = data.groupBy("killedBaronNashor")
  .agg(avg("hasWon").alias("winRate"))
baronImpact.show()


// COMMAND ----------

val wardsDestroyedImpact = data.groupBy("hasWon")
  .agg(avg("wardsDestroyed").alias("avgWardsDestroyed"))
wardsDestroyedImpact.show()


// COMMAND ----------

val wardsPlacedImpact = data.groupBy("hasWon")
  .agg(avg("wardsPlaced").alias("avgWardsPlaced"))
wardsPlacedImpact.show()


// COMMAND ----------

val inhibitorImpact = data.select(
  col("destroyedTopInhibitor").alias("topInhibitor"),
  col("destroyedMidInhibitor").alias("midInhibitor"),
  col("destroyedBotInhibitor").alias("botInhibitor"),
  col("hasWon")
).groupBy()
 .agg(
   avg("topInhibitor").alias("topImpact"),
   avg("midInhibitor").alias("midImpact"),
   avg("botInhibitor").alias("botImpact")
 )
inhibitorImpact.show()


// COMMAND ----------

val durationImpact = data.groupBy("hasWon")
  .agg(avg("gameDuration").alias("avgGameDuration"))
durationImpact.show()


// COMMAND ----------

val firstTowerImpact = data.groupBy("isFirstTower")
  .agg(avg("hasWon").alias("winRate"))
firstTowerImpact.show()


// COMMAND ----------


