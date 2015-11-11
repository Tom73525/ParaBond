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
import parabond.value.SimpleBondValuator
import parabond.util.Helper
import parabond.casa.MongoConnection
import com.mongodb.client.MongoCursor
import parabond.casa.MongoHelper
import parabond.casa.MongoDbObject

/**
 * This class runs a map-reduce unit test for n portfolios in the
 * parabond database. It uses one portfolio per actor for the first
 * portfolios.
 * @author Ron Coleman, Ph.D.
 */
class Mr00 {
  /** Gets a connection to the parabond database */
  val mongo = MongoConnection(MongoHelper.getHost)("parabond")
  
  def test {
    // Create the input of a list of Tuple2(portf id, curve coefficients).
    val input = (1 to 4).foldLeft(List[(Int,List[Double])]()) { (list, p) =>
      list ::: List((p,Helper.curveCoeffs))
    }
    
    // Run the map-reduce
    val t0 = System.nanoTime
    
    val result = MapReduce.mapreduceBasic(input, mapping, reducing)
    
    val t1 = System.nanoTime
    
    println("%6s %10.10s".format("PortId","Value"))

    // Generate the report by portfolio with the run-time
    result.foreach {
      case(portfId, values) =>
      	println("%6d %10.2f".format(portfId,values(0)))        
    }
    
    val dt = (t1 - t0) / 1000000000.0
    
    println("dt = %f".format(dt))    
  }
  
  /**
   * Mapping function
   * @param portId Portfolio id
   * @param fitter Curve fitting coefficients
   * @returns List of (portf id, bond value))
   */
  def mapping(portfId: Int, fitter: List[Double]): List[(Int,Double)] = {
    val portfsCollecton = mongo("Portfolios")
    
    val portfsQuery = MongoDbObject("id" -> portfId)

    val portfsCursor = portfsCollecton.find(portfsQuery)
    
    val bondIds = MongoHelper.asList(portfsCursor,"instruments")
    
    val bondsCollection = mongo("Bonds")

    val t0 = System.nanoTime
    
    val value = bondIds.foldLeft(0.0) { (sum, id) =>
      val bondsQuery = MongoDbObject("id" -> id)

      val bondsCursor = bondsCollection.find(bondsQuery)

      val bond = MongoHelper.asBond(bondsCursor)

//      print("bond(" + bond + ") = ")
      
      val valuator = new SimpleBondValuator(bond, fitter)

      val price = valuator.price

//      println("%8.2f".format(price))
      
      sum + price
    }    
    
    val t1 = System.nanoTime
    
    val dt = (t1 - t0) / 1000000000.0
    
    println("value = %10.2f bonds = %d dt = %f".format(value,bondIds.size,dt))
    
    List((portfId, value))
  }
  
  /**
   * Trivial reduce function since the portfolio is already reduced.
   * @param portfId Portfolio id
   * @param vals Bond valuations
   * @returns List of portfolio valuation, one per portfolio
   */
  def reducing(portfId: Int,vals: List[Double]): List[Double] = {
    List(vals(0))
  }
}