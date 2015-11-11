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

import parabond.casa.MongoHelper
import parabond.entry.SimpleBond
import parabond.util.Helper
import parabond.value.SimpleBondValuator
import scala.util.Random
import parabond.casa.MongoDbObject

/**
 * This class uses parallel collections to price n portfolios in the
 * parabond database using the memory-bound coarse-grain algorithm.
 * @author Ron Coleman, Ph.D.
 */
class Par07 {
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
    
    val me =  this.getClass().getSimpleName()
    val outFile = me + "-dat.txt"
    
    val fos = new java.io.FileOutputStream(outFile,true)
    val os = new java.io.PrintStream(fos)
    
    os.print(me+" "+ "N: "+n+" ")
    
    val details = if(System.getProperty("details") != null) true else false
    
    // Build the portfolio list 
        
    val numCores = Runtime.getRuntime().availableProcessors()
    
    val coarseInputs = (1 to numCores).foldLeft(List[List[Data]]()) { (xs,x) =>
          val inputs = for(i <- 0 until (n / numCores)) yield Data(ran.nextInt(100000)+1,null, null) 
                   
          inputs.toList :: xs 
    }
    
    val t2 = System.nanoTime
    val chunks = coarseInputs.par.map(loadChunk)
    val t3 = System.nanoTime     

    // Build the portfolio list
    val now = System.nanoTime  
    val results = chunks.par.map(price) 
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
    
    os.print("dt(1): %7.4f  dt(N): %7.4f  cores: %d  R: %5.2f  e: %5.2f ".
        format(dt1,dtN,numCores,speedup,e))  
    
    os.println("load t: %8.4f ".format((t3-t2)/1000000000.0)) 
    
    os.flush
    
    os.close
    
    println(me+" DONE! %d %7.4f".format(n,dtN))      
  }
  
  def price(inputs: List[Data]) : List[Data] = {   
    val outputs = inputs.foldLeft(List[Data]()) { (xs, input) =>
      val t0 = System.nanoTime
    
      val portfId = input.portfId
    
      val bonds = input.bonds
    
      // Price each bond and sum all the prices
      val value = bonds.foldLeft(0.0) { (sum, bond) =>    
          // Price the bond
          val valuator = new SimpleBondValuator(bond, Helper.curveCoeffs)

          val price = valuator.price
      
          // The price into the aggregate sum
          sum + price
      }    
    
      MongoHelper.updatePrice(portfId,value) 
      
      val t1 = System.nanoTime
    
      Data(portfId,null,Result(portfId,value,bonds.size,t0,t1)) :: xs     
    }
 
    outputs
  }  
  
  def loadChunk(inputs: List[Data]) : List[Data] = {   
    val outputs = inputs.foldLeft(List[Data]()) { (xs, input) =>
      val t0 = System.nanoTime
      
      val portfId = input.portfId
      
      val portfsQuery = MongoDbObject("id" -> portfId)

      val portfsCursor = MongoHelper.portfCollection.find(portfsQuery)
    
      // Get the bonds ids in the portfolio
      val bondIds = MongoHelper.asList(portfsCursor,"instruments")
    
      // Price each bond and sum all the prices
      val bonds = bondIds.foldLeft(List[SimpleBond]()) { (list, id) =>
        // Get the bond from the bond collection
        val bondQuery = MongoDbObject("id" -> id)

        val bondCursor = MongoHelper.bondCollection.find(bondQuery)

        val bond = MongoHelper.asBond(bondCursor)
        
        list ++ List(bond)
      }    
      
      xs ++ List(Data(portfId,bonds,null))
    }
    
    outputs
  }
  
  /**
   * Parallel load the portfolios with embedded bonds.
   * Note: This version uses parallel fold to reduce all the
   */
  def loadPortfsParFold(n: Int): List[Data] = {    
    // Initialize the portfolios to retrieve
    val portfs = for(i <- 0 until n) yield Data(ran.nextInt(100000)+1,null,null) 
    
    val z = List[Data]()
    
    val list = portfs.par.fold(z) { (a,b) =>
      // Make a into list (it already is one but this tells Scala it's one)
      // Seems a = z initially
      val opa = a match {
        case y : List[_] =>
          y
      }
      
      b match {
        // If b is a list, just append the two lists
        case opb : List[_] =>
          opb ++ opa
        
        // If b is a data, append the data to the list
        case x : Data =>
          val intermediate = MongoHelper.fetchBonds(x.portfId) 
          
          List(Data(x.portfId,intermediate.list,null)) ++ opa
      }         

    }
    
    list match {
      case l : List[_] =>
        l.asInstanceOf[List[Data]]
      case _ =>
        List[Data]()
    }
  }    
  
}