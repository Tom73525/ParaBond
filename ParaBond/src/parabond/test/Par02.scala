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

import scala.actors.Actor.actor
import scala.actors.Actor.receive
import scala.actors.Actor.self
import scala.util.Random
import parabond.entry.SimpleBond
import parabond.casa.MongoHelper
import parabond.util.Helper
import parabond.value.SimpleBondValuator
import scala.collection.mutable.ListBuffer

/**
 * This class runs a parallel collections unit test for n portfolios in the
 * parabond database. It uses one portfolio per map by loading all the bonds
 * into memory.
 * @author Ron Coleman
 */
class Par02 {
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
    
    print("\n"+this.getClass()+" "+ "N: "+n+" ")
    
    val details = if(System.getProperty("details") != null) true else false
    
    val t2 = System.nanoTime
    val input = loadPortfsPar2(n)
    val t3 = System.nanoTime   
    
    // Build the portfolio list
    val now = System.nanoTime  
    val results = input.par.map(priced) 
    input.par.reduce { (a: Data, b:Data) =>
      Data(0,null,Result(0,a.result.price + b.result.price,0,0,0))
    }
    val t1 = System.nanoTime
    
    // Generate the output report
    if(details)
      println("%6s %10.10s %-5s %-2s".format("PortId","Price","Bonds","dt"))
    
    val dt1 = results.foldLeft(0.0) { (sum,result) =>      
      sum + (result.result.t1 - result.result.t0)
      
    } / 1000000000.0
    
    val dtN = (t1 - now) / 1000000000.0
    
    val speedup = dt1 / dtN
    
    val numCores = Runtime.getRuntime().availableProcessors()
    
    val e = speedup / numCores
    
    println("dt(1): %7.4f  dt(N): %7.4f  cores: %d  R: %5.2f  e: %5.2f ".
        format(dt1,dtN,numCores,speedup,e))   
    
    println("load t: %8.4f ".format((t3-t2)/1000000000.0))       
  }
  
  def priced(input: Data): Data = {
    
    // Value each bond in the portfolio
    val t0 = System.nanoTime
    
    val outputs = input.bonds.par.map { bond =>
      val valuator = new SimpleBondValuator(bond, Helper.curveCoeffs)

      val price = valuator.price
      
      new SimpleBond(bond.id,bond.coupon,bond.freq,bond.tenor,price)      
    }
    
    val bondsValue = outputs.par.reduce { (a: SimpleBond, b:SimpleBond) =>
      new SimpleBond(0,0,0,0,a.maturity+b.maturity)
    }  
    
    MongoHelper.updatePrice(input.portfId,bondsValue.maturity)  
    
    val t1 = System.nanoTime
    
    Data(input.portfId,null,Result(input.portfId,bondsValue.maturity,input.bonds.size,t0,t1))
  }  
  
  def finePrice(bond: SimpleBond): SimpleBond = {
      val valuator = new SimpleBondValuator(bond, Helper.curveCoeffs)

      val price = valuator.price
      
      new SimpleBond(bond.id,bond.coupon,bond.freq,bond.tenor,price)
  }
  
  /**
   * Parallel load the portfolios with embedded bonds.
   */
  def loadPortfsPar(n: Int): List[Data] = {   
    val lotteries = for(i <- 0 to n) yield ran.nextInt(100000)+1 
    
    val list = lotteries.par.foldLeft (List[Data]())
    { (portfIdBonds,portfId) =>
      val intermediate = MongoHelper.fetchBonds(portfId)
      
      Data(portfId,intermediate.list,null) :: portfIdBonds
    }
    
    list
  }  
  
  
    /**
   * Parallel load the portfolios and bonds into memory (actor-based).
   */
  def loadPortfsPar2(n : Int) : ListBuffer[Data] = {
    import scala.actors._
    import Actor._

    val caller = self
      
    (1 to n).foreach { p =>
      
      // Select a portfolio
      val lottery = ran.nextInt(100000) + 1
      
      actor {
        caller ! MongoHelper.fetchBonds(lottery)
      }
    }

    val list = ListBuffer[Data]()

    (1 to n).foreach { p =>
      receive {
        case MongoHelper.Intermediate(portfId, bonds) =>
          list.append(Data(portfId, bonds,null))
      }
    }

    list

  }
}