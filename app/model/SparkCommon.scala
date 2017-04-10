package model

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession 
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import scala.concurrent.ExecutionContext.Implicits.global

//System.setProperty("hadoop.home.dir", "c:\winutil\")
/**
  * Created by ved on 21/11/15.
  */
  
  
object SparkCommon {
 

    //lazy val spark = SparkSession.builder().appName("SparkSession").master("spark://ubuntu-spark:7077").getOrCreate()
    lazy val spark = SparkSession.builder().appName("SparkSession").master("local[*]").getOrCreate()
 
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
  
   


    // for converting CancelType to CanceltypeRating
    val stringToRating: (String => Int) = (arg: String) => 
        {
             if (arg == "No Show") 1 
             else if (arg == "Clinic Cancelled") 4
             else if (arg == "Patient Cancelled") 2
             else if (arg == "User Error") 7
             else if (arg == "Show On Schedule") 3
             else if (arg == "Deleted Visit") 6
             else if (arg == "Cancelled Appt") 5
             else if (arg == "MISSED APPT") 1
             else 8
    }
    val reasonType_udf = udf(stringToRating)
    //UDF for extracting integer from vector
    val vectorHead = udf{ x:org.apache.spark.ml.linalg.Vector => x(0) }

   
 
  def toJsonString(rdd:DataFrame):String =
    "["+rdd.toJSON.take(1).toList.mkString(",\n")+"]"
    
    //192.168.100.168
    
    val connectStr: String = "jdbc:postgresql://192.168.100.168:55432/integra_1"
    
    /* creating dtaframe from two .csv files */
    val t_patients_schedule =  spark.read.format("jdbc").option("url",connectStr ).option("dbtable", "dbo.t_patients_schedule").option("user", "postgres").option("password", "postgres").load()
                                //.withColumn("day",dayofweek(col("scheduleendtime")))
                                .withColumn("hour",hour(col("schedulebegintime")))
                                .withColumn("minute",minute(col("schedulebegintime")))
                                .withColumn("dayofmonth",dayofmonth(col("schedulebegintime")))
    //val ratingsDF = SparkCommon.spark.read.option("header","true").option("inferSchema", "true").csv("raw_data/ratings.csv")
    
    val t_facility_roles =  spark.read.format("jdbc").option("url", connectStr).option("dbtable", "dbo.t_facility_roles").option("user", "postgres").option("password", "postgres").load() 
   
    val t_users_facility_role =  spark.read.format("jdbc").option("url", connectStr).option("dbtable", "dbo.t_users_facility_role").option("user", "postgres").option("password", "postgres").load() 

    val t_address =  spark.read.format("jdbc").option("url", connectStr).option("dbtable", "dbo.t_address").option("user", "postgres").option("password", "postgres").load()
 
    val t_roles =  spark.read.format("jdbc").option("url", connectStr).option("dbtable", "dbo.t_roles").option("user", "postgres").option("password", "postgres").load()
 
    val t_userroles = t_facility_roles.join(t_users_facility_role , "facilityroleguid" ).select("userguid","roleguid").distinct

    val new_roles = t_roles.filter(col("description") === "Doctor")

    val only_DTN = t_userroles.join(new_roles, "roleguid")
    
    val preProc3 = t_patients_schedule.join(only_DTN, "userguid")

    val raw_data = preProc3.groupBy("patientguid","userguid").count()


    val patient_schedule_df_raw = raw_data.join(t_address , raw_data.col("patientguid")===t_address.col("ForeignKeyGuid"),"left")
                            .withColumnRenamed("city","patientCity").withColumnRenamed("state","patientState").withColumnRenamed("zip","patientZip")
                            .select("patientguid","userguid","patientCity","patientState","patientZip","count").distinct()
                            .join(t_address , raw_data.col("userguid")===t_address.col("ForeignKeyGuid"),"left")
                            .withColumnRenamed("city","userCity").withColumnRenamed("state","userState").withColumnRenamed("zip","userZip")
   
    
   val patient_schedule_df = patient_schedule_df_raw.join(t_patients_schedule,  patient_schedule_df_raw.col("patientguid")===t_patients_schedule.col("patientguid"), "inner").drop(t_patients_schedule.col("patientguid")).drop(t_patients_schedule.col("userguid")).withColumn("cancelAtrrib", reasonType_udf(col("canceltype"))).filter("canceltype is null")
                                .cache
                                
                                
    val features =  patient_schedule_df.select("patientguid", "userguid")
    
    
    
  
    
    
    
    //StringIndexer to get the dictionary ride of features

    val stringIndexers: Array[org.apache.spark.ml.PipelineStage] = features.columns.map(colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(s"${colName}_index")
        )
    val assembler = new VectorAssembler()
      .setInputCols(Array("count"))
      .setOutputCol("raw_rating")
      
   
    
    val scaler = new MinMaxScaler().setMin(1.0).setMax(10.0)
      .setInputCol("raw_rating")
      .setOutputCol("scaled_Rating")
    
    val pipeline = new Pipeline().setStages(Array(stringIndexers(0),stringIndexers(1),assembler,scaler))
    
    val raw_data_rating_scaled = pipeline.fit(patient_schedule_df).transform(SparkCommon.patient_schedule_df)
    
    
    
    val raw_data_rating_int = raw_data_rating_scaled.withColumn("rating", vectorHead(raw_data_rating_scaled("scaled_Rating"))).drop("scaled_Rating").drop("raw_rating")
    
    val raw_data_rating_before_spilt = raw_data_rating_int.select("patientguid_index","userguid_index","rating","patientguid","userguid","facilityguid").distinct()
        
}