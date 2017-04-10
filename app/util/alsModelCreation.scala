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
import scala.util.Random


/*@Inject() extends Controller*/

@Singleton
class alsModelCreation  {
 
 
    

    val Array(training_data, validat_data, test_data) = SparkCommon.raw_data_rating_before_spilt.randomSplit(Array(0.6,0.2,0.2), 1123)
    training_data.cache
     
    val als_schedul = new ALS()
                    .setRank(10)
                    .setMaxIter(10)
                    .setRegParam(0.6)
                    //.setAlpha(15)
                    //.setImplicitPrefs(true)
                    .setUserCol("patientguid_index")
                    .setItemCol("userguid_index")
                    .setRatingCol("rating")
                    
    val model_schedul = als_schedul.fit(training_data)
    
    
    val evaluator_schedul = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
      
      
    // Evaluate the model by computing the RMSE on the test data
    val predictions = model_schedul.transform(validat_data)
    val predictions_n = predictions.na.drop()
     
    // model_schedul.save("raw_data/recModel")
     
    //val rmse_schedul = evaluator_schedul.evaluate(predictions_n)
    //println(s"Root-mean-square error = $rmse_schedul")
      //  val preProc3 = t_patients_schedule.join(only_DTN, "userguid")
      
      /*def model = Action {
        Ok(SparkCommon.toJsonString(predictions))
      }*/
      
      /*
     // Evaluate the model by computing the RMSE on the test data
    val RMSEpredictions = model_ml.transform(testDF)
    
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(RMSEpredictions)
    println(s"Root-mean-square error = $rmse")
      
      */
  
   
  
  

}