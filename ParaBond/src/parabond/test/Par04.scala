package parabond.test

import parabond.mongo.MongoHelper
import parabond.mongo.MongoConnection
import parabond.entry.SimpleBond
import parabond.value.SimpleBondValuator
import parabond.util.Helper
import scala.util.Random
import parabond.mongo.MongoDbObject
import com.mongodb.client.MongoCursor

/**
 * This class uses parallel collections to price n portfolios in the
 * parabond database using the composite coarse-grain algorithm.
 * @author Ron Coleman, Ph.D.
 */
class Par04 {
  /** Number of bond portfolios to analyze */
  val PORTF_NUM = 100
  
  /** Initialize the random number generator */
  val ran = new Random(0)   
  
  /** Write a detailed report */
  val details = false
  
  /** Record captured with each result */
  case class Result(id : Int, price: Double, bondCount: Int, t0: Long, t1: Long)
  
  case class Data(portfId: Int, bonds:List[SimpleBond], result: Result)
  
  def test {
    // Set the number of portfolios to analyze
    val arg = System.getProperty("n")
    
    val n = if(arg == null) PORTF_NUM else arg.toInt
    
    var me =  this.getClass().getSimpleName()
    var outFile = me + "-dat.txt"
    
    var fos = new java.io.FileOutputStream(outFile,true)
    var os = new java.io.PrintStream(fos)
    
    os.print(me+" "+ "N: "+n+" ")
    
    val details = if(System.getProperty("details") != null) true else false
    
    // Build the portfolio list 
        
    val numCores = Runtime.getRuntime().availableProcessors()
    
    val coarseInputs = (1 to numCores).foldLeft(List[List[Data]]()) { (xs,x) =>
          val inputs = for(i <- 0 until (n / numCores)) yield Data(ran.nextInt(100000)+1,null, null) 
          
//          println("inner inputs sz = "+inputs.size+" "+inputs.getClass())
         
          inputs.toList :: xs 
    }
    
    // Build the portfolio list
    val now = System.nanoTime  
    val results = coarseInputs.par.map(priced) 
    val t1 = System.nanoTime
    
    // Generate the output report
    if(details)
      println("%6s %10.10s %-5s %-2s".format("PortId","Price","Bonds","dt"))
    
    val dt1 = results.foldLeft(0.0) { (sum,result) =>
      result.foldLeft(0.0) { (sm,rslt) =>
        sm + (rslt.result.t1 - rslt.result.t0)
      } + sum
    } / 1000000000.0
    
    val dtN = (t1 - now) / 1000000000.0
    
    val speedup = dt1 / dtN

    
    val e = speedup / numCores
    
    os.println("dt(1): %7.4f  dt(N): %7.4f  cores: %d  R: %5.2f  e: %5.2f ".
        format(dt1,dtN,numCores,speedup,e))  
    
    os.flush
    
    os.close
    
    println(me+" DONE! %d %7.4f".format(n,dtN))      
  }
  
  def priced(inputs: List[Data]) : List[Data] = {   
    val outputs = inputs.foldLeft(List[Data]()) { (xs, x) =>
      val t0 = System.nanoTime
      
      val portfId = x.portfId
      
      val portfsQuery = MongoDbObject("id" -> portfId)

      val portfsCursor = MongoHelper.portfCollection.find(portfsQuery)
    
      // Get the bonds ids in the portfolio
      val bondIds = MongoHelper.asList(portfsCursor,"instruments")
    
      // Price each bond and sum all the prices
      val value = bondIds.foldLeft(0.0) { (sum, id) =>
        // Get the bond from the bond collection
        val bondQuery = MongoDbObject("id" -> id)

        val bondCursor = MongoHelper.bondCollection.find(bondQuery)

        val bond = MongoHelper.asBond(bondCursor)
      
        // Price the bond
        val valuator = new SimpleBondValuator(bond, Helper.curveCoeffs)

        val price = valuator.price
      
        // The price into the aggregate sum
        sum + price
      }    
    
      MongoHelper.updatePrice(portfId,value) 
      
      val t1 = System.nanoTime
    
      Data(portfId,null,Result(portfId,value,bondIds.size,t0,t1)) :: xs     
    }
 
    outputs
  }  
}