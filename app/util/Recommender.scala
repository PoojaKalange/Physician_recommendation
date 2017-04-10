package util

import model.SparkCommon
import org.apache.spark.sql.DataFrame
import javax.inject._
import play.api._
import play.api.mvc._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS



@Singleton
class Recommender  {
    
   val modelCreation  = new alsModelCreation()

  
      def userRecomend(Patient_guid:Int,Facilityt_guid:String) : DataFrame = {
          
        val temp_test_data_lit = modelCreation.test_data.drop("patientguid_index")
                            .withColumn("patientguid_index", lit(Patient_guid))
                            .filter(modelCreation.test_data.col("facilityguid")===Facilityt_guid)
          
        val  cleanTest_recommend_per_user = modelCreation.model_schedul.transform(temp_test_data_lit).na.fill(0)
        val recommendation_id = cleanTest_recommend_per_user.orderBy(cleanTest_recommend_per_user.col("prediction").desc).limit(20)  
          
           
        val recommend_shecdule_id = recommendation_id.join(SparkCommon.raw_data_rating_int, SparkCommon.raw_data_rating_int.col("patientguid_index") === recommendation_id.col("patientguid_index"), "left")
                                    .drop(SparkCommon.raw_data_rating_int.col("userguid")).drop(SparkCommon.raw_data_rating_int.col("patientguid")).drop(SparkCommon.raw_data_rating_int.col("patientguid_index"))
        
        return recommend_shecdule_id.orderBy(recommend_shecdule_id.col("prediction").desc).select("patientguid","userguid","prediction").distinct()
          
          
      }
      
 
   
  
 
     val result =  modelCreation.model_schedul.transform(modelCreation.test_data)
     
    
 
   

}




































