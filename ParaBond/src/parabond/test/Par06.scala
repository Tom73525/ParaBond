package parabond.test

import scala.collection.mutable.ListBuffer
import scala.util.Random

import parabond.entry.SimpleBond
import parabond.mongo.MongoConnection
import parabond.mongo.MongoHelper
import parabond.util.Helper
import parabond.value.SimpleBondValuator

/**
 * This class uses parallel collections to price n portfolios in the
 * parabond database using the fine-grain memory-bound algorithm.
 * @author Ron Coleman, Ph.D.
 */
class Par06 {
  /** Number of bond portfolios to analyze */
  val PORTF_NUM = 100
        
  /** Connects to the parabond DB */
  val mongo = MongoConnection(MongoHelper.getHost)("parabond")
  
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
    
    // Load all the bonds into into memory
    // Note: the input is a list of Data instances, each element of which contains a list
    // of bonds
    val t2 = System.nanoTime
    val inputs = loadPortfsParFold(n)
    val t3 = System.nanoTime   
    
    // Build the portfolio list
    val now = System.nanoTime  
    val outputs = inputs.par.map(priced) 
    val t1 = System.nanoTime
    
    // Generate the detailed output report
    if(details) {
      println("%6s %10.10s %-5s %-2s".format("PortId","Price","Bonds","dt"))
      
      outputs.foreach { output =>
        val id = output.result.id

        val dt = (output.result.t1 - output.result.t0) / 1000000000.0

        val bondCount = output.result.bondCount

        val price = output.result.price

        println("%6d %10.2f %5d %6.4f %12d %12d".format(id, price, bondCount, dt, output.result.t1 - now, output.result.t0 - now))
      }
    }
    
    val dt1 = outputs.foldLeft(0.0) { (sum,result) =>      
      sum + (result.result.t1 - result.result.t0)
      
    } / 1000000000.0
    
    val dtN = (t1 - now) / 1000000000.0
    
    val speedup = dt1 / dtN
    
    val numCores = Runtime.getRuntime().availableProcessors()
    
    val e = speedup / numCores
    
    os.print("dt(1): %7.4f  dt(N): %7.4f  cores: %d  R: %5.2f  e: %5.2f ".
        format(dt1,dtN,numCores,speedup,e))  
    
    os.println("load t: %8.4f ".format((t3-t2)/1000000000.0)) 
    
    os.flush
    
    os.close
    
    println(me+" DONE! %d %7.4f".format(n,dtN))       
  }
  
  def priced(input: Data): Data = {
    
    // Value each bond in the portfolio
    val t0 = System.nanoTime
   
//    val bondIds = asList(portfsCursor,"instruments")
    val bonds = input.bonds
    
    val output = input.bonds.par.map { bond =>
      val t0 = System.nanoTime 
      
      val valuator = new SimpleBondValuator(bond, Helper.curveCoeffs)

      val price = valuator.price
      
      val t1 = System.nanoTime
      
      new SimpleBond(bond.id,bond.coupon,bond.freq,bond.tenor,price)     
    }.par.reduce(sum)
    
    MongoHelper.updatePrice(input.portfId,output.maturity) 
    
    val t1 = System.nanoTime
    
    Data(input.portfId,null,Result(input.portfId,output.maturity,input.bonds.size,t0,t1))
  }  

  def sum(a: SimpleBond, b:SimpleBond) : SimpleBond = {
    new SimpleBond(0,0,0,0,a.maturity+b.maturity)
  } 

  /**
   * Parallel load the portfolios with embedded bonds.
   * Note: This version uses parallel fold to reduce all the
   */
  def loadPortfsParFold(n: Int): List[Data] = {
    val portfsCollection = mongo("Portfolios")
    
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
          val intermediate = MongoHelper.fetchBonds(x.portfId, portfsCollection) 
          
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