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
import scala.util.Random
import za.co.absa.spline.core.DataLineageListener

object LinearTimeTest extends App {
  val spark = SparkSession.builder().appName("Spline perf test - constant time").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val data = spark.read.parquet("file:///home/jan/wiki100m")

  import za.co.absa.spline.core.SparkLineageInitializer._
  spark.enableLineageTracking()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  data.cache
  data.count

  val ops = Array(
    (x: Dataset[Row]) => { x.withColumn("page_title", upper($"page_title")) },
    (x: Dataset[Row]) => { x.withColumn("page_title", lower($"page_title")) },
    (x: Dataset[Row]) => { x.withColumn("domain_code", lower($"domain_code")) },
    (x: Dataset[Row]) => { x.withColumn("domain_code", upper($"domain_code")) },
    (x: Dataset[Row]) => { x.withColumn("count_views", $"domain_code" + Random.nextInt()) },
    (x: Dataset[Row]) => { x.withColumn("total_response_size", $"total_response_size" + Random.nextInt()) },
    (x: Dataset[Row]) => { x.withColumn(s"calcColumn" + Math.abs(Random.nextInt()), dayofmonth($"date")) },
    (x: Dataset[Row]) => { x.withColumn(s"calcColumn" + Math.abs(Random.nextInt()), dayofyear($"date")) },
    (x: Dataset[Row]) => { x.withColumn(s"calcColumn" + Math.abs(Random.nextInt()), hour($"date")) },
    (x: Dataset[Row]) => { x.filter(locate(Random.nextPrintableChar().toString(), $"page_title") > 0) },
    (x: Dataset[Row]) => { x.filter(year($"date") === 2016) })

  def applyRandomOp(times: Int, d: Dataset[Row]) = {
    (0 until times).toArray.foldLeft(d)({
      case (df, i) =>
        val op = ops(i % ops.length)
        op(df)
    })
  }

  //run 10 times before testing - warm up
  (0 until 10).toList.foreach({ x =>
    val t = applyRandomOp(Random.nextInt(100) * x, data)
    println(s"Pre-run $x, ${t.count}")
  })

  Thread.sleep(5000)
  DataLineageListener.clearMeasurements()

  for (numOps <- Range(0, 1001, 100)) yield {
    val t = applyRandomOp(numOps, data)

    println(s"Run $numOps, ${t.count}")

  }

}