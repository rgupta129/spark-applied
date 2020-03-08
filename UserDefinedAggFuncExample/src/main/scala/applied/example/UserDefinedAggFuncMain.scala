package applied.example

import org.apache.spark.sql.SparkSession

case class IdScore(id: Long, score: Int)
/**
  * Main method
  */
object UserDefinedAggFuncMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val df = List(
      IdScore(1, 10), IdScore(1, 0), IdScore(1, 10), IdScore(1, 20), IdScore(1, 10),
      IdScore(2, 50), IdScore(2, 100), IdScore(2, 10), IdScore(2, 20), IdScore(2, 10)
    ).toDF()

    df.groupBy($"id").agg(new UserDefinedAggFuncExample(2)($"score")).show()
    df.groupBy($"id").agg(new UserDefinedAggFuncExample(3)($"score")).show()
    df.groupBy($"id").agg(new UserDefinedAggFuncExample(4)($"score")).show()

    spark.stop()

  }

}
