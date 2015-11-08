/*
 * Copyright (c) Scaly Contributors
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Scaly Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package parabond.test

import parabond.mr.MapReduce
import parabond.mongo.MongoHelper
import parabond.mongo.MongoConnection
import parabond.util.Helper
import scala.util.Random
import parabond.value.SimpleBondValuator
import parabond.mongo.MongoDbObject

/**
 * This class runs a map-reduce unit test for n portfolios in the
 * parabond database. It uses the coarse-gain method of n portfolio
 * spread across m cores.
 * @author Ron Coleman, Ph.D.
 */
class Mr02 {
  /** Number of bond portfolios to analyze */
  val PORTF_NUM = 100
        
  /** Connects to the parabond DB */
  val mongo = MongoConnection(MongoHelper.getHost)("parabond")
  
  /** Record captured with each result */
  case class Result(id : Int, price: Double, bondCount: Int, t0: Long, t1: Long)
  
  /** Initialize the random number generator */
  val ran = new Random(0)   
  
  /** Get the number of cores */
  val numCores = Runtime.getRuntime().availableProcessors()  
  
  /** Write a detailed report */
  val details = true
  
  /** Unit test entry point */
    def test {
    // Set the number of portfolios to analyze
    val arg = System.getProperty("n")
    
    val n = if(arg == null) PORTF_NUM else arg.toInt
    
    val me =  this.getClass().getSimpleName()
    val outFile = me + "-dat.txt"
    
    val fos = new java.io.FileOutputStream(outFile,true)
    val os = new java.io.PrintStream(fos)
    
    os.print(me+" "+ "N: "+n+" ") 
    
    val details = if(System.getProperty("details") != null) true else false
    
    // Build the portfolio list
    val input = (1 to n).foldLeft(List[(Int,List[Double])]()) { (list, p) =>
      val r = ran.nextInt(100000)+1
      list ::: List((r,Helper.curveCoeffs))
    }
    
    // Map-reduce the input
    val now = System.nanoTime
    
    val resultsUnsorted = MapReduce.coarseMapReduce(input, mapping, reducing,numCores,numCores)
//    val resultsUnsorted = MapReduce.mapreduceBasic(input, mapping, reducing)
  
    val t1 = System.nanoTime
    
    // Generate the output report
    if(details)
    	println("%6s %10.10s %-5s %-2s".format("PortId","Price","Bonds","dt"))

    val list = resultsUnsorted.foldLeft(List[Result]()) { (list, rsult) =>
      val (portfId, result) = rsult
      
      list ::: List(result(0))
    }
    
    val results = list.sortWith(_.t0 < _.t0)

    if (details)
      results.foreach { result =>
        val id = result.id

        val dt = (result.t1 - result.t0) / 1000000000.0

        val bondCount = result.bondCount

        val price = result.price

        println("%6d %10.2f %5d %6.4f %12d %12d".format(id, price, bondCount, dt, result.t1 - now, result.t0 - now))
      }
    
    val dt1 = results.foldLeft(0.0) { (sum,result) =>      
      sum + (result.t1 - result.t0)
      
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
  
  /**
   * Maps a portfolio to a single price
   * @param portId Portfolio id
   * @param fitter Curve fitting coefficients
   * @returns List of (portf id, bond value))
   */
  def mapping(portfId: Int, fitter: List[Double]): List[(Int,Result)] = {
    // Value each bond in the portfolio
    val t0 = System.nanoTime
    
    // Connect to the portfolio collection
    val portfsCollecton = mongo("Portfolios")
    
    // Retrieve the portfolio 
    val portfsQuery = MongoDbObject("id" -> portfId)

    val portfsCursor = portfsCollecton.find(portfsQuery)
    
    // Get the bonds in the portfolio
    val bondIds = MongoHelper.asList(portfsCursor,"instruments")
    
    // Connect to the bonds collection
    val bondsCollection = mongo("Bonds")
    
    val value = bondIds.foldLeft(0.0) { (sum, id) =>
      // Get the bond from the bond collection
      val bondQuery = MongoDbObject("id" -> id)

      val bondCursor = bondsCollection.find(bondQuery)

      val bond = MongoHelper.asBond(bondCursor)

//      print("bond(" + bond + ") = ")
      
      // Price the bond
      val valuator = new SimpleBondValuator(bond, fitter)

      val price = valuator.price

//      println("%8.2f".format(price))
      
      // The price into the aggregate sum
      sum + price
    }    
    
    val t1 = System.nanoTime
    
    List((portfId, Result(portfId,value,bondIds.size,t0,t1)))
  }
  
  /**
   * Reduces trivially portfolio prices.
   * Since there's only one price per porfolio, there's nothing
   * really to reduce! 
   * @param portfId Portfolio id
   * @param vals Bond valuations
   * @returns List of portfolio valuation, one per portfolio
   */
  def reducing(portfId: Int,vals: List[Result]): List[Result] = {
    List(vals(0))
  }
}