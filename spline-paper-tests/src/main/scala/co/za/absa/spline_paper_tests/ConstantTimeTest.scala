//Copyright 2017 Jan Scherbaum
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package co.za.absa.spline_paper_tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import za.co.absa.spline.core.DataLineageListener
import org.scalameter._

object ConstantTimeTest extends App {
  val spark = SparkSession.builder().appName("Spline perf test - constant time").getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  
  if (args(0).toBoolean) {
    import za.co.absa.spline.core.SparkLineageInitializer._
    spark.enableLineageTracking()
  }

  val datasets = (for (i <- Range(10, 101, 10)) yield {
    val df = spark.read.parquet(s"file:///home/jan/wiki${i}m")
    (i, df)
  }).toMap

  import spark.implicits._
  import org.apache.spark.sql.functions._

  // warm up
  Range(10, 101, 10).toList.foreach({ x =>
    val t = performCalc(datasets(x))
    println(s"Pre-run $x, ${t.count}")
  })

  Thread.sleep(5000)
  DataLineageListener.clearMeasurements()

  //perform some calculations
  def performCalc(df: Dataset[Row]) = df.filter(year($"date") === 2016).filter($"domain_code" === "en").groupBy(hour($"date") as "hourOfDay").agg(sum($"count_views") as "countViews")

  val times = for (x <- Range(10, 101, 10)) 
    yield measure {
    
    println(s"Run $x, ${performCalc(datasets(x)).count()}")
  }
  
  times.foreach(println)
  
}