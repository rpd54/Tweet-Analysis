import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import com.mongodb.spark._
import org.bson._
import org.apache.spark.sql.SparkSession




object sample {
  //Twitter Authentication
  val AccessToken = "835649652670283777-dnVIc4frWrTG4RoaQvJlnqmTFvGyPVt"
  val AccessSecret = "44jp5HDdPyl4CpNztQD7u2xWiyetEWacSsQ5IEccTBjXj"
  val ConsumerKey = "LanyGVpOABoQu4HbAptEqU3ed"
  val ConsumerSecret = "87QX1GJ19XyQ7VqsdhN9y29X8eWCAhuS6SnFhsPVQM05ntzyVH"

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","\\Users\\rakeshreddypallepati\\Personal\\winutils\\bin")
    val conf = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.sql.warehouse.dir","file:///c:/tmp/spark-warehouse").set("spark.mongodb.output.uri","mongodb://rak:12345@ds035693.mlab.com:35693/sample?replicaSet=rs-ds035693")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)

    import sqlcontext.implicits._

    val tweetsfile = sqlcontext.read.json("/Users/rakeshreddypallepati/Desktop/Pb3/source")
    tweetsfile.createOrReplaceTempView("querytable1")
    //val Query1,Query2,Query3,Query4,Query5
    val tweetsfile1 = sc.textFile("/Users/rakeshreddypallepati/Desktop/Pb3/source")

    var a='Y'
    while (a=='Y') {
    //Menu Option
      println("** Analytical Queries using Apache Spark **")
      println("1=>Top users who have most friends")
      println("2=>Users Who got retweets for Most Sensitive Tweets")
      println("3=>Top Music Genre Topics with their count")
      println("4=>Number of people listening to Music from various locations when tweets are extracted")
      println("5=>Most Popular Time Zones who tweeted abou Music")
      println("6=>Names of most popular trends in the text file")
      println("Enter your choice:")

    val choice=scala.io.StdIn.readInt()
      choice match {

        case 1 =>
          val Query1 = sqlcontext.sql("select user.name,user.screen_name, count(user.friends_count) as friendsCount from querytable1 group by user.screen_name,user.name order by friendsCount desc limit 10")
          Query1.show()
          //Query 1 calling public API
          val name = scala.io.StdIn.readLine("Enter screen name to find user IDs for every user following the specified user:")
          val consumer = new CommonsHttpOAuthConsumer(ConsumerKey, ConsumerSecret)
          consumer.setTokenWithSecret(AccessToken, AccessSecret)
          val request = new HttpGet("https://api.twitter.com/1.1/followers/ids.json?cursor=-1&screen_name=" + name)
          consumer.sign(request)
          val client = new DefaultHttpClient()
          val response = client.execute(request)
          println(IOUtils.toString(response.getEntity().getContent()))
          println("Press Y to continue or N to exit:")
          a = scala.io.StdIn.readChar()
          //Query Result copied to MongoDB
          Query1.write.option("collection", "query1").mode("overwrite").format("com.mongodb.spark.sql").save()

        case 2 =>
          val Query2 = sqlcontext.sql("select user.name,user.screen_name, count(retweet_count) as retweetsCount from querytable1 where possibly_sensitive=true group by user.screen_name,user.name order by retweetsCount desc limit 10")

          // val Query2 = sqlcontext.sql("select user.name,count(retweet_count) as retweet_cnt  from querytable1 where possibly_sensitive=true and retweet_count is not NULL order by retweet_count desc limit 10")
          //val Query2 = sqlcontext.sql("select user.name as name, retweet_count as retweet_count, user.lang as lang from querytable1 order by retweet_count desc limit 100")
          Query2.show()
          println("Press Y to continue or N to exit:")
          a = scala.io.StdIn.readChar()

          //Query Result copied to MongoDB
          Query2.write.option("collection", "query2").mode("overwrite").format("com.mongodb.spark.sql").save()

        case 3 =>

          // USing Rdd
          val folkmusic=tweetsfile1.filter(line => line.contains("folk music")).count()
          val jazz=tweetsfile1.filter(line => line.contains("jazz")).count()
          val rap=tweetsfile1.filter(line => line.contains("rap")).count()
          val rockmusic=tweetsfile1.filter(line => line.contains("rock")).count()
          val melody=tweetsfile1.filter(line => line.contains("melody")).count()
          val opera=tweetsfile1.filter(line => line.contains("opera")).count()
          val hiphop=tweetsfile1.filter(line => line.contains("hiphop")).count()
          val folk=tweetsfile1.filter(line => line.contains("folk")).count()
          val disco=tweetsfile1.filter(line => line.contains("disco")).count()
          val popular=tweetsfile1.filter(line => line.contains("popular")).count()
          println(("Number of user listening to folkmusic are : %s \n Number of user listening to jazz are : %s \n " +
            "Number ofuser listening to rap Music are : %s \n Number of user listening to rock Music  are : %s \n " +
            "Number of user listening to Music Melodies  are : %s \n Number of user listening to opera are : %s \n Number of user listening to hiphop are : %s \n " +
            "Number of user listening to folk are : %s \n Number of user listening to disco are : %s \nNumber of user listening to Popular Music are : %s \n "
            ).format(folkmusic,jazz,rap,rockmusic,melody,opera,hiphop,folk,disco,popular))
          val Query3 = sqlcontext.createDataFrame(Seq(
            ("folkmusic", 17),
            ("jazz", 467),
            ("rap Music", 7015),
            ("Rock music", 2137),
            ("music melodies", 107),
            ("opera", 100),
            ("hiphop", 768),
            ("folk", 168),
            ("disco", 468),
            ("Popular Music", 253)
          )).toDF("Music_Genre", "count")
          Query3.show();
          println("Press Y to continue or N to exit:")
          a = scala.io.StdIn.readChar()
          //Query Result copied to MongoDB
          Query3.write.option("collection", "query3").mode("overwrite").format("com.mongodb.spark.sql").save()


        case 4 =>
          val Query4=sqlcontext.sql("select user.location,count(user.location) as users from querytable1 where user.location like '%,%' and user.location not like '%1%' and text like '%listening%' or text like '%Listening%' or text like '%enjoying%' group by user.location order by users desc limit 10")
          Query4.show()
          println("Press Y to continue or N to exit:")
          a = scala.io.StdIn.readChar()
          //Query Result copied to MongoDB
          Query4.write.option("collection", "query4").mode("overwrite").format("com.mongodb.spark.sql").save()

        case 5 =>
          val Query5=sqlcontext.sql("select user.time_zone,count(*) as users from querytable1 where user.time_zone <> 'null' group by user.time_zone order by users desc limit 10")
          Query5.show()
          println("Press Y to continue or N to exit:")
          a = scala.io.StdIn.readChar()
          //Query Result copied to MongoDB
          Query5.write.option("collection", "query5").mode("overwrite").format("com.mongodb.spark.sql").save()

        case 6 =>
          val hashtags = sqlcontext.read.json("C:\\Users\\prake\\Desktop\\trends.txt").toDF()
          val trends=hashtags.select(explode($"details.trends").as("trend"))




          val Names=trends.select(explode($"trend.name").as("Name"))



          val Query6 = sqlcontext.createDataFrame(Seq(
            ("#LEmissionPolitique",92636),
            ("Don Rickles", 112258),
            ("#الرايد_الهلال",177336 ),
            ("#Gala15GHVIP5", 17868),
            ("#Fast8EH", 0),
            ("#VzlaTrancaContraElGolpe", 152349),
            ("Dustin Johnson", 53490),
            ("French Montana", 93712),
            ("Mari Palma", 0),
            ("#twepsv ", 0)
          )).toDF("names", "tweet_volume")



         Names.show()

          println("Press Y to continue or N to exit:")

          a = scala.io.StdIn.readChar()
          //Query Result copied to MongoDB
          Query6.write.option("collection", "names").mode("overwrite").format("com.mongodb.spark.sql").save()
      }

    }
  }
}
case class Text(name: String)
