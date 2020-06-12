
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.LongAccumulator
import org.apache.spark.rdd._
import java.io.File
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Orange {

/////////////////////////////////////////////////////////////////////////////////////
// 1) Write functions to read and write (from hive and aws s3)
/////////////////////////////////////////////////////////////////////////////////////

  def readDF(spark:SparkSession,pathString:String, header:Boolean):DataFrame = {
    val df = spark.read.option("header",header).csv(pathString)
    df
  }
  def writeDF(df:DataFrame, pathString:String, header:Boolean):Unit = {
    df.write
      .format("com.databricks.spark.csv")
      .option("header", header)
      .save(pathString)
  }


/////////////////////////////////////////////////////////////////////////////////////
// leer desde hive
// val query = spark.sql("select * from input_table")
/////////////////////////////////////////////////////////////////////////////////////
// leer desde S3 sería algo similar leer desde HDFS, pero indicando el protocolo "s3a://", no tengo ningun S3 para leer, asi que continuo con HDFS
// faltaria el tema de Kerberos y el acceso al S3
/////////////////////////////////////////////////////////////////////////////////////
// val df = spark.read.option("header",true).csv("s3a:///bucket/data/input.csv")
/////////////////////////////////////////////////////////////////////////////////////


  def main (args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)

	// Create a SparkSession. No need to create SparkContext
	// You automatically get it as part of the SparkSession
	val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
	val spark = SparkSession
	   .builder()
	   .appName("PruebaOrange")
	   .config("spark.sql.warehouse.dir", warehouseLocation)
	   .enableHiveSupport()
	   .getOrCreate()



	/////////////////////////////////////////////////////////////////////////////////////
	// 2) Email providers with more than 10k posts
	/////////////////////////////////////////////////////////////////////////////////////

	//el fichero tiene header, lo leemos como dataframe y saltamos la cabecera
	val df = spark.read.option("header",true).csv("/user/evinhas1/data/input.csv")

	//los nombres de los campos tienen un espacio al principio....

	//anhadimos una columna con el proveedor de correo
	val df_provider = df.withColumn("email_provider", split(col(" email"),"@").getItem(1))

	df_provider.createOrReplaceTempView("provider")

	//esto devolvería los email providers que tienen más de 10K posts (ninguno, el que más tiene, tiene 233)
	val df_email_provider = spark.sql("select email_provider, sum(` posts`) as no_of_posts from provider group by email_provider having sum(` posts`)>10000")

	/////////////////////////////////////////////////////////////////////////////////////
	// 3) Post by email providers
	/////////////////////////////////////////////////////////////////////////////////////

	//create a df with the email provider and the number of posts

	val df_posts_by_email_provider = spark.sql("select email_provider, sum(` posts`) as no_of_posts from provider group by email_provider")

	df_posts_by_email_provider.createOrReplaceTempView("posts")

	// join the previous DF to the main DF (df_provider)
	val df_join = spark.sql("select p.*, pt.no_of_posts from provider p left outer join posts pt on p.email_provider = pt.email_provider")


	val df_clean = df_join.withColumnRenamed(" name", "name").withColumnRenamed(" email", "email").withColumnRenamed(" joined", "joined").withColumnRenamed(" ip_address", "ip_address").withColumnRenamed(" posts", "posts").withColumnRenamed(" bday_day", "bday_day").withColumnRenamed(" bday_month", "bday_month").withColumnRenamed(" bday_year", "bday_year").withColumnRenamed(" members_profile_views", "members_profile_views").withColumnRenamed(" referred_by", "referred_by")

	df_clean.createOrReplaceTempView("df_clean")

	/////////////////////////////////////////////////////////////////////////////////////
	// 4) Year/s with max sign ups
	/////////////////////////////////////////////////////////////////////////////////////


	val df_year = spark.sql("select member_id, from_unixtime(joined,'yyyy') as joined_year from df_clean")

	df_year.createOrReplaceTempView("year")

	val new_join = spark.sql("select clean.*, year.joined_year from df_clean clean left outer join year on clean.member_id = year.member_id  ")

	new_join.createOrReplaceTempView("new_join")


	//calculate the max sign ups year
	val years_max_sign_ups = spark.sql("select max(number),year from (select count(*) as number, joined_year as year from new_join group by joined_year) as t group by year")

	/////////////////////////////////////////////////////////////////////////////////////
	// 5) Class C IP address frequency by 1 st octet
	/////////////////////////////////////////////////////////////////////////////////////

	val df_ip_octet = new_join.withColumn("ip_first_octet", split(col("ip_address"),"\\.").getItem(0))


	df_ip_octet.createOrReplaceTempView("df_ip_octet")


	val df_aux = spark.sql("select count(*) frequency_ip_1st_octet, ip_first_octet from df_ip_octet group by ip_first_octet")

	df_aux.createOrReplaceTempView("df_aux")

	val df_5 = spark.sql("select octet.*, aux.frequency_ip_1st_octet from df_ip_octet as octet left outer join df_aux as aux on octet.ip_first_octet = aux.ip_first_octet")


	df_5.createOrReplaceTempView("df_5")

	/////////////////////////////////////////////////////////////////////////////////////
	// 6) Frequency of IP address based on first 3 octets
	/////////////////////////////////////////////////////////////////////////////////////

	val df_ip_3_octet = df_5.withColumn("ip_3_octet", concat(split(col("ip_address"),"\\.").getItem(0), split(col("ip_address"),"\\.").getItem(1),  split(col("ip_address"),"\\.").getItem(2)))


	df_ip_3_octet.createOrReplaceTempView("df_ip_3_octet")

	val df_aux2 = spark.sql("select count(*) frequency_ip_3_octet, ip_3_octet from df_ip_3_octet group by ip_3_octet")


	df_aux2.createOrReplaceTempView("df_aux2")

	val df_6 = spark.sql("select octet.*, aux.frequency_ip_3_octet from df_ip_3_octet as octet left outer join df_aux2 as aux on octet.ip_3_octet = aux.ip_3_octet")


	df_6.createOrReplaceTempView("df_6")


	/////////////////////////////////////////////////////////////////////////////////////
	// 7) Number of referral by members
	/////////////////////////////////////////////////////////////////////////////////////
	val df_aux3 = spark.sql("select df_6.member_id, df_6.referred_by from df_6 ")

	df_aux3.createOrReplaceTempView("df_aux3")

	val df_7 = spark.sql("select count(*) no_of_referrals, referred_by as member from df_aux3 group by referred_by")

	df_7.show


  }



}
