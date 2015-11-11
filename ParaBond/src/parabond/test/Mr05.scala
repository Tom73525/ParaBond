/*
 * Copyright (c) Ron Coleman
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
import parabond.casa.MongoHelper
import parabond.casa.MongoConnection
import parabond.util.Helper
import com.mongodb.client.MongoCursor
import scala.util.Random
import parabond.value.SimpleBondValuator
import parabond.casa.MongoDbObject


/**
 * This class runs a map-reduce unit test for n portfolios in the
 * parabond database. It alternates between parallel and serial methods.
 * @author Ron Coleman, Ph.D.
 */
class Mr05 {
  /** Number of bond portfolios to analyze */
  val PORTF_NUM = 100
        
  /** Connects to the parabond DB */
  val mongo = MongoConnection("127.0.0.1")("parabond")
  
  /** Record captured with each result */
  case class Result(id : Int, price: Double, bondCount: Int, t0: Long, t1: Long)
  
  /** Initialize the random number generator */
  val ran = new Random(0)   
  
  /** Write a detailed report */
  val details = true
  
  /** Unit test entry point */
  def test {
    // Set the number of portfolios to analyze
    val arg = System.getProperty("n")
    
    val n = if(arg == null) PORTF_NUM else arg.toInt
    
    print("\n"+this.getClass()+" "+ "N: "+n +" ")
    
    val details = if(System.getProperty("details") != null) true else false
        
    (1 to n).foreach { p =>
      // Choose a random portfolio
      val lottery = ran.nextInt(100000)+1 

      val results = new Array[Result](2)
      
      // Evaluate the portfolios in random order
      if(lottery %2 == 0) {
        results(0) = priceParallel(lottery)
        results(1) = priceSerially(lottery)
      }
      else {
        results(1) = priceSerially(lottery)         
        results(0) = priceParallel(lottery)
      }
      
      val dt0 = (results(0).t1 - results(0).t0) / 1000000000.0
      
      val dt1 = (results(1).t1 - results(1).t0) / 1000000000.0      
        
      println("%6d %10.2f %10.2f %6.4f %6.4f".format(results(0).id, results(0).price, results(1).price, dt0, dt1))
      
    }
  }
  
  def priceParallel(portfId : Int) : Result = {
    val t0 = System.nanoTime
    
    val list = List((portfId,Helper.curveCoeffs))
    
    val result = MapReduce.mapreduceBasic(list, mapping, reducing)  
    
    val t1 = System.nanoTime
    
    val rsult = result(portfId)(0)
    
    Result(portfId,rsult.price,rsult.bondCount,t0,t1)
  }
  
  def priceSerially(portfId : Int) : Result = {
      // Connect to the portfolio collection
      val t0 = System.nanoTime
      
      val portfsCollecton = mongo("Portfolios")
      
      val portfsQuery = MongoDbObject("id" -> portfId)

      val portfsCursor = portfsCollecton.find(portfsQuery)

      // Get the bonds in the portfolio
      val bondIds = MongoHelper.asList(portfsCursor, "instruments")

      // Connect to the bonds collection
      val bondsCollection = mongo("Bonds")

      val value = bondIds.foldLeft(0.0) { (sum, id) =>
        
        // Get the bond from the bond collection
        val bondQuery = MongoDbObject("id" -> id)

        val bondCursor = bondsCollection.find(bondQuery)

        val bond = MongoHelper.asBond(bondCursor)

        // Price the bond
        val valuator = new SimpleBondValuator(bond, Helper.curveCoeffs)

        val price = valuator.price

        // Add the price into the aggregate sum
        sum + price
      }    
      
      val t1 = System.nanoTime
      Result(portfId,value,bondIds.size,t0,t1)
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