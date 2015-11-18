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

import scala.util.Random

import parabond.casa.MongoConnection
import parabond.casa.MongoHelper
import parabond.util.Helper
import parabond.value.SimpleBondValuator

/** Test driver */
object NPortfolio101 {
  def main(args: Array[String]): Unit = {
    new NPortfolio01 test
  }
}

/**
 * This class implements the memory-bound serial algorithm.
 * @author Ron Coleman, Ph.D.
 */
class NPortfolio01 {
  /** Number of bond portfolios to analyze */
  val PORTF_NUM = 100

  /** Connects to the parabond DB */
  val mongo = MongoConnection(MongoHelper.getHost)("parabond")
  
  /** Initialize the random number generator */
  val ran = new Random(0)  
  
  /** Write a detailed report */
  val details = true
  
  /** Record captured with each result */
  case class Result(id: Int, price: Double, bondCount: Int, t0: Long, t1: Long)

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
    
    val t2 = System.nanoTime
    val input = MongoHelper.loadPortfs(n)
    val t3 = System.nanoTime 
    
    val t0 = System.nanoTime
    
    val results = input.foldLeft(List[Result]()) { (sum, p) =>  
      // Value each bond in the portfolio
      val t0 = System.nanoTime

      // Retrieve the portfolio 
      val (portfId, bonds) = p

      val value = bonds.foldLeft(0.0) { (sum, bond) =>
        
        // Price the bond
        val valuator = new SimpleBondValuator(bond, Helper.curveCoeffs)

        val price = valuator.price

        // Add the price into the aggregate sum
        sum + price
      }
      
      MongoHelper.updatePrice(portfId,value)
      
      val t1 = System.nanoTime
      
      Result(portfId,value,bonds.size,t0,t1) :: sum     
    }
    
    val t1 = System.nanoTime
  
    // Generate the output report
    if (details) {
      println("%6s %10.10s %-5s %-2s".format("PortId", "Price", "Bonds", "dt"))

      results.reverse.foreach { result =>
        val id = result.id

        val dt = (result.t1 - result.t0) / 1000000000.0

        val bondCount = result.bondCount

        val price = result.price

        println("%6d %10.2f %5d %6.4f %12d %12d".format(id, price, bondCount, dt, result.t1 - t0, result.t0 - t0))
      }
    }
    
    val dt1 = results.foldLeft(0.0) { (sum,result) =>      
      sum + (result.t1 - result.t0)
      
    } / 1000000000.0
    
    
    val dtN = (t1 - t0) / 1000000000.0
    
    val speedup = dt1 / dtN
    
    val numCores = Runtime.getRuntime().availableProcessors()
    
    val e = 1.0
    
    os.print("dt(1): %7.4f  dt(N): %7.4f  cores: %d  R: %5.2f  e: %5.2f ".
        format(dt1,dtN,numCores,speedup,e))     
    
    os.println("load t: %8.4f ".format((t3-t2)/1000000000.0))   
    
    os.flush
    
    os.close
    
    println(me+" DONE! %d %7.4f".format(n,dtN))     
  }
  
}