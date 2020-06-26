package com.project.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object fifaProject {
    val spark = SparkSession.builder().appName("fifa project").master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val fifaDF = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Users\\Devanand\\Desktop\\datafifa.csv")

    // a. Which club has the most number of left footed midfielders under 30 years of age?

    val preferredFootAge = fifaDF.where("PreferredFoot = 'Left' and age <= 30")
    val clubWithMostLeftFooters = preferredFootAge.groupBy(col("club")).agg(count("club").alias("clubcount")).orderBy(desc("clubcount"))
    clubWithMostLeftFooters.show(1)

    // b.  The strongest team by overall rating for a 4-4-2 formation?

    val strongClub = fifaDF.groupBy(col("club")).agg(avg("overall").alias("cluboverallrating")).orderBy(desc("cluboverallrating"))
    strongClub.show(1)

    //c. Which team has the most expensive squad value in the world? Does that team also have the largest wage bill ?

    val averageValueByClub = fifaDF.groupBy(col("club")).agg(avg("Value").alias("averageValueByClub"))

    val expensiveSquadValue = averageValueByClub.agg(max("averageValueByClub").alias("expensiveSquadValue"))

    val averageWageByClub = fifaDF.groupBy(col("club")).agg(avg("Wage").alias("averageWageByClub"))

    val highestWageByClub = averageWageByClub.agg(max("averageWageByClub").alias("highestWageByClub"))

    //may return null output as wage and value columns are not integer with symbols
    //From the resultset, when a club has "highest wage by club" and "expensiveSquadValue" then
    // the club also has largest wage bill otherwise not.


    //d. Which position pays the highest wage in average?
    val highestWageByPosition = fifaDF.groupBy(col("Position")).agg(avg("Wage").alias("averageWageByPosition")).orderBy(desc("averageWageByPosition"))
    highestWageByPosition.show(1)  //may return null output as wage is not integer with euro symbol


  }
}
