package controllers

import javax.inject._
import play.api._
import play.api.mvc._
import model.SparkCommon
import util.{alsModelCreation,Recommender}
/* import scalaj.http._*/

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
 
@Singleton
class Applications @Inject() extends Controller {

 val alsModel = new alsModelCreation()
 val recommender = new Recommender()
 
  def index = Action {
    Ok("Your new application is ready. predicted recommendations according to specific user's taste in the json format ")
  }

    
 
/*val response = Http("http://devopenemr.carelink360.com/openemr/api/testuserforAI.php")
  .header("key", "val").method("get")
  .execute().asString.body    
*/


    
  def model = Action {
        Ok(SparkCommon.toJsonString(alsModel.predictions))
      }  
  
  
   def recDoctor(patient_ID:Int,Facility_ID:String) = Action {
       
        val temp = recommender.userRecomend(patient_ID,Facility_ID)
        Ok(SparkCommon.toJsonString(temp))
      }     
  
  def   df = Action {
        Ok(SparkCommon.toJsonString(recommender.result))
      }    
      

}
