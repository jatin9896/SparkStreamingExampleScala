import java.util.{Locale, ResourceBundle}

import org.apache.log4j.Logger
import twitter4j.conf.ConfigurationBuilder
import twitter4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object StreamingExample {

  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    val connection=getConnection()
    //val list:ResponseList[Status] =connection.getHomeTimeline()
    println(connection.getHomeTimeline())
   /* val spark = SparkSession
      .builder
      .appName("CarbonDataStreaming")
      .getOrCreate()

    import spark.implicits._
    val lines =spark.readStream
    val dataframes: DataFrame =lines.text(connection.getHomeTimeline().toString)
    println("data frames : "+dataframes)*/
  }

  private def getConnection(): Twitter = {
    try {
      val property = ResourceBundle.getBundle("twitterconfig", Locale.getDefault)
      val configurationBuilder = new ConfigurationBuilder
      configurationBuilder.setDebugEnabled(true)
        .setOAuthConsumerKey(property.getString("consumerkey"))
        .setOAuthConsumerSecret(property.getString("consumersecret"))
        .setOAuthAccessToken(property.getString("accesstoken"))
        .setOAuthAccessTokenSecret(property.getString("accesstokensecret"))
      val twitter = new TwitterFactory(configurationBuilder.build()).getInstance()
      println("Connection Established")
      twitter
    }
    catch {
      case ex: TwitterException => throw new TwitterException("Connection failed")
    }
  }

  private def printTweets(status: List[Status]) = status foreach (x => logger.debug("User Name : " + x.getUser.getName + "\t" + "Status : " + x.getText))




}
