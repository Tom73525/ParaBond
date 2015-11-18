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
import parabond.entry.SimpleBond
import parabond.util.Helper
import parabond.value.SimpleBondValuator
import com.mongodb.client.MongoCursor
import parabond.casa.MongoDbObject

/** Test driver */
object Mr06 {
  def main(args: Array[String]): Unit = {
    new Mr06 test
  }
}

/**
 * This class runs a map-reduce unit test first loading the portfolios to memory.
 * @author Ron Coleman, Ph.D.
 */
class Mr06 {
  /** Number of bond portfolios to analyze */
  val PORTF_NUM = 100

  /** Connects to the parabond DB */
  val mongo = MongoConnection("127.0.0.1")("parabond")      
      
  /** Record captured with each result */
  case class Result(id: Int, price: Double, bondCount: Int, t0: Long, t1: Long)

  /** Write a detailed report */
  val details = true

  /** Unit test entry point */
  def test {
    // Set the number of portfolios to analyze
    val arg = System.getProperty("n")

    val n = if (arg == null) PORTF_NUM else arg.toInt

    println("\n" + this.getClass() + " " + "N = " + n)

    val details = if (System.getProperty("details") != null) true else false

    val t2 = System.nanoTime
    val input = MongoHelper.loadPortfsParallel(n)
    val t3 = System.nanoTime
    
    println(" portf load time: %8.4f".format((t3-t2)/1000000000.0))

    // Map-reduce the input
    val t0 = System.nanoTime

    val resultsUnsorted = MapReduce.mapreduceBasic(input, mapping, reducing)

    val t1 = System.nanoTime

    // Generate the output report
    if (details)
      println("%6s %10.10s %-5s %-2s".format("PortId", "Price", "Bonds", "dt"))

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

        println("%6d %10.2f %5d %6.4f %12d %12d".format(id, price, bondCount, dt, result.t1 - t0, result.t0 - t0))
      }

    val dt1 = results.foldLeft(0.0) { (sum, result) =>
      sum + (result.t1 - result.t0)

    } / 1000000000.0

    val dtN = (t1 - t0) / 1000000000.0

    val speedup = dt1 / dtN

    val numCores = Runtime.getRuntime().availableProcessors()

    val e = speedup / numCores

    println("dt(1): %7.4f  dt(N): %7.4f  cores: %d  R: %5.2f  e: %5.2f ".
      format(dt1, dtN, numCores, speedup, e))
  }
 
  /**
   * Maps a portfolio to a single price.
   * @param portId Portfolio id
   * @param fitter Curve fitting coefficients
   * @returns List of (portf id, bond value))
   */
  def mapping(portfId: Int, bonds : List[SimpleBond]): List[(Int, Result)] = {
    val t0 = System.nanoTime
    
    val price = bonds.foldLeft(0.0) { (sum, bond) =>
      val valuator = new SimpleBondValuator(bond, Helper.curveCoeffs)

      val price = valuator.price

      sum + price      
    }
    
    // Update the portfolio with the value
    
    val ids = bonds.map(p => p.id)
    
    val entry = MongoDbObject("id" -> portfId, "instruments" -> ids, "value" -> price)
    
    mongo("Portfolios").insertOne(entry)
    
    val t1 = System.nanoTime
    
    List((portfId, Result(portfId,price,bonds.size,t0,t1)))
  }

  /**
   * Reduces trivially portfolio prices.
   * Since there's only one price per porfolio, there's nothing
   * really to reduce!
   * @param portfId Portfolio id
   * @param vals Bond valuations
   * @returns List of portfolio valuation, one per portfolio
   */
  def reducing(portfId: Int, vals: List[Result]): List[Result] = {
    List(vals(0))
  }
}