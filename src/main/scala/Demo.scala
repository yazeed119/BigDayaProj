import org.apache.spark
import org.apache.log4j.BasicConfigurator

//import org.apache.spark.sql.SQLContext.implicits._
import scala.collection.Searching.Found
import scala.math.Fractional.Implicits.infixFractionalOps
import scala.math.Integral.Implicits.infixIntegralOps
import scala.reflect.internal.util.NoFile.file
import scala.reflect.internal.util.NoPosition.line
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

object Demo {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val conf = new SparkConf()
    val sc = new SparkContext()

    val sqlC = new org.apache.spark.sql.SQLContext(sc)
    import sqlC.implicits._

    val rddWhole = sc.wholeTextFiles("C:\\DataSet\\Dataset\\*")
    val data =rddWhole.map(X => X._1)
    val revenuePerOrder = rddWhole

  val columns = Seq("language","users_count")

    //val dfFromRDD2 = spark.createDataFrame(rddWhole).toDF(columns:_*)
    val dfFromRDD = rddWhole.toDF()
    val RddWhole = sc.parallelize(
      Seq(
        ("Word"),
        ("Count(Word)"),
        ("Document_list")
      )
    )
    println(rddWhole.getClass)
    rddWhole.foreach(f=>{
      println(f._1+"=>"+f._2)
    })
    rddWhole.saveAsTextFile("C:\\DataSet\\Dataset\\wholeInvertedIndex.txt")

    val RDD = sc.textFile("C:\\DataSet\\Dataset\\*")
    val counts = RDD.flatMap(line => line.split(" "))
    RDD.collect().foreach(println)
    val rdd=sc.parallelize(Seq(("Word"),
      ("Count(Word)"),
      ("Doument_list")))
    rdd.foreach(println)
    rdd.collect().foreach(println)

    val input = scala.io.StdIn.readInt()
    println("Enter a query " + input)
    val word= rdd.filter(line=>line.contains(input)).count
    println(word)

  }
}

