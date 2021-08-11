package com.cummins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import com.cummins.util.Job
import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, TimestampType}
import org.joda.time.DateTime

/**
 * movie_analysis
 * Create by sm487 on 2021/8/9
 * READ : [users.dat] [movies.dat] [ratings.dat]
 * SAVE : [TableName]
 */
object movieJob extends Job{
  override def run(): Unit = {



    import spark.implicits._


    val users = spark.read.option("sep","`").csv("rescourse/users.dat").toDF("UserID","Gender","Age","Occupation","Zipcode")
      .withColumn("UserID",col("UserID").cast(LongType))
      .withColumn("Age",col("Age").cast(IntegerType))
    val movies = spark.read.option("sep","`").csv("rescourse/movies.dat").toDF("MovieID","Title","Genres")
      .withColumn("MovieID",col("MovieID").cast(LongType))
    val ratings = spark.read.option("sep","`").csv("rescourse/ratings.dat").toDF("UserID","MovieID","Rating","Timestamped")
      .withColumn("UserID",col("UserID").cast(LongType))
      .withColumn("MovieID",col("MovieID").cast(LongType))
      .withColumn("Rating",col("Rating").cast(DoubleType))
      .withColumn("Timestamped",from_unixtime($"Timestamped"))


    //求被评分次数最多的10部电影，并给出评分次数（电影名，评分次数）
    println("-----求被评分次数最多的10部电影，并给出评分次数（电影名，评分次数）-----")
    val mostcounttop10 = ratings.join(movies, movies.col("MovieID") === ratings.col("MovieID"), "left")
      .drop(movies.col("MovieID"))
      .groupBy("MovieID", "Title")
      .count().
      sort(col("count").desc)
      .limit(10)

      //.show(false)//show里面加false不会截断信息
    //ratings.join(movies,col("MovieID"),"left").show(false)//show里面加false不会截断信息

    //  分别求男性，女性当中评分最高的10部电影（性别，电影名，影评分）
    val rating_M = ratings.join(movies, movies.col("MovieID") === ratings.col("MovieID"), "left")
      .join(users, users.col("UserID") === ratings.col("UserID"), "left").drop(movies.col("MovieID")).drop(users.col("UserID"))
      .where($"Gender" === "M").groupBy("Gender", "Title").agg(avg("Rating") as "avg_rating").sort($"avg_rating".desc)
      .limit(10)


    val rating_F = ratings.join(movies, movies.col("MovieID") === ratings.col("MovieID"), "left")
      .join(users, users.col("UserID") === ratings.col("UserID"), "left").drop(movies.col("MovieID")).drop(users.col("UserID"))
      .where($"Gender" === "F").groupBy("Gender", "Title").agg(avg("Rating") as "avg_rating").sort($"avg_rating".desc)
      .limit(10)
    println("-----分别求男性，女性当中评分最高的10部电影（性别，电影名，影评分）-----")
    //rating_M.unionByName(rating_F).show(false)
    //    4、求movieid = 2116这部电影各年龄段（因为年龄就只有7个，就按这个7个分就好了）的平均影评（年龄段，影评分）
    println("-----求movieid = 2116这部电影各年龄段（因为年龄就只有7个，就按这个7个分就好了）的平均影评（年龄段，影评分）-----")
    //     ratings.join(users,users.col("UserID") === ratings.col("UserID"),"left")
    //      .drop(users.col("UserID"))
    //      .where($"MovieID"=== "2116")
    //      .groupBy("Age")
    //      .agg(avg("rating")as("ave_ratings")).show(false)

    //    计算最喜欢看电影（影评次数最多）的那位女性评最高分的10部电影的平均影评分（观影者，电影名，影评分）
    println("-----计算最喜欢看电影（影评次数最多）的那位女性评最高分的10部电影的平均影评分（观影者，电影名，影评分）-----")
    val most_femal: Array[Any] = ratings.join(users,users.col("UserID") === ratings.col("UserID"),"left")
      .drop(users.col("UserID"))
      .where($"Gender"==="F").groupBy("UserID").count().sort($"count".desc).select("UserID").limit(1).collect().map(_(0))
    val most_people = most_femal(0)
    val top10 = ratings.where($"UserID"===most_people).orderBy($"Rating".desc).limit(10)
    //top10.orderBy("MovieID").show(false)
    val movie_avg = movies.join(ratings, movies.col("MovieID") === ratings.col("MovieID"), "left")
      .drop(ratings.col("MovieID"))
      .groupBy("MovieID", "Title","Genres")
      .agg(avg("Rating") as "avg_ratings")

    val res = top10.join(movie_avg,movie_avg.col("MovieID")===top10.col("MovieID"),"left")
      .drop(top10.col("MovieID"))
    //      res.select("UserID","Title","MovieID","avg_ratings")
    //         .show(false)


    //    计算每个年份的最好看（被评分平均分最高的）的Top10部电影
    println("-----计算每个年份的最好看（被评分平均分最高的）的Top10部电影-----")
    val movie_year = movie_avg.withColumn("year",substring($"Title",-5,4))
    val window_year = Window.partitionBy("year").orderBy($"avg_ratings".desc)
    // movie_year.withColumn("pm",row_number() over(window_year)).where($"pm"<11).select("year","Title","avg_ratings").show(false)

    //    计算1997年上映的电影中，评分最高的10部Comedy类电影
    println("-----计算1997年上映的电影中，评分最高的10部Comedy类电影-----")
  movie_year
      .withColumn("judgment", lit("Comedy"))
      .withColumn("judgment_result", when(instr(col("Genres"), "Comedy") === 0, 0).otherwise(1))
      .where($"judgment_result" === 1)
      .where($"year" === "1997")
      .orderBy($"avg_ratings".desc)
      .select("Title","Genres","avg_ratings","year")
      .limit(10)

    // comedy_top10.show(false)

    //    计算该影评库中各种类型电影中评价最高的5部电影（类型，电影名，平均影评分）
    println("-----计算该影评库中各种类型电影中评价最高的5部电影（类型，电影名，平均影评分）-----")

    val genres_window = Window.partitionBy("Genres").orderBy($"avg_ratings".desc)
    val gesnres_movie = movie_avg.withColumn("Genres", explode(split($"Genres", "\\|")))
    val gesnres_result = gesnres_movie.withColumn("count", row_number() over genres_window)
      .where($"count" < 6)
      .select("Genres","Title","avg_ratings")

    //    各年评分最高的电影类型（年份，类型，影评分）
    println("-----各年评分最高的电影类型（年份，类型，影评分）-----")

    val year_window = Window.partitionBy("year").orderBy($"avg_ratings".desc)
    movie_year.withColumn("count",row_number()over year_window)
      .where($"count"===1)
      .select("year","Genres","avg_ratings")
    //    每个地区（邮政编码）最高评分的电影名
    println("-----每个地区（邮政编码）最高评分的电影名-----")
    val zip_movie = movies.join(ratings, movies.col("movieID") === ratings.col("movieID"), "left")
      .join(users, ratings.col("UserID") === users.col("UserID"), "left")
      .select("Title", "Rating", "Zipcode")
    val zip_avg = zip_movie.groupBy("Title", "Zipcode")
      .agg(avg("Rating") as "avg_ratings")
    val zip_window = Window.partitionBy("Zipcode").orderBy($"avg_ratings".desc)
    val zip_result = zip_avg.withColumn("row", row_number() over (zip_window))
      .where($"row" === 1)
      .drop("row")
    //    计算产生评分次数最多的那一天，每10分钟统计当天的累计评价数（也就是每十分钟的累计数都代表从0点到当前时刻为止的总计 数，因此该曲线肯定是单调递增的）
    val most_date = ratings.withColumn("Timestamped", to_date($"Timestamped"))
      .groupBy("Timestamped").agg(count($"Timestamped") as ("count"))
      .orderBy($"count".desc)
      .select("Timestamped")
      .limit(1).collect().map(_ (0))
    var day = most_date(0)
    //    println(day)
    val most_day: DataFrame = ratings.withColumn("year", to_date($"Timestamped"))
      .where($"year" === day).drop("year")
    val mostdaycount = most_day.groupBy(window($"Timestamped", "10 minutes")as("time"))
      .agg(count("Timestamped") as ("count"))
      val time_window = Window.orderBy("time")
    val time_result = mostdaycount.withColumn("sum",sum("count") over(time_window))
    //    计算2001年这一年每两周、四周的移动平均值(MA2、MA4)
    val resultyear = ratings.withColumn("year", year($"Timestamped"))
      .withColumn("Timestamped",to_date($"Timestamped"))
      .where($"year" === "2001").drop("year")

    val MA2 = resultyear.groupBy(window($"Timestamped", "2 week", "2 week", "10 day ") as ("2week"))
      .agg(count("Timestamped") as ("2week_count"))
      .orderBy("2week")
      .withColumn("2week_count",$"2week_count".cast(DoubleType))


     val MA2_window = Window.rowsBetween(-1,Window.currentRow)
    MA2.withColumn("MA2",avg("2week_count") over(MA2_window))show(false)




    val MA4 = resultyear.groupBy(window($"Timestamped", "4 week", "4 week", "10 day") as ("4week"))
      .agg(count("Timestamped") as ("4week_count"))
      .orderBy($"4week")
      .withColumn("4week_count",$"4week_count".cast(DoubleType))

    val MA4_window = Window.rowsBetween(-1,Window.currentRow)
    MA4.withColumn("MA4",avg("4week_count") over(MA4_window))show(false)

    //    计算产生评分次数最多的那一天每半小时的评分次数形成评分次数走势（即每隔半小时统计前半个小时的评价总数，提示：可用基于时间序列的滚动窗口）

       most_day.groupBy(window($"Timestamped","30 minutes")as("half-hour"))
         .agg(count("Timestamped"))
         .orderBy("half-hour")



  }

}
