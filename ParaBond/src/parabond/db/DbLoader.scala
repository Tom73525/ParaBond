package parabond.db

import parabond.mongo.MongoConnection
import scala.io.Source
import parabond.mongo.MongoDbObject

/**
 * @author Ron.Coleman
 */
object DbLoader {
  /** Portfolios collection name */
  val COLL_PORTFOLIOS = "Portfolios"

  /** Bonds collection name */
  val COLL_BONDS = "Bonds"

  /** Bonds input data */
  val INPUT_BONDS = "bonds.txt"

  /** Portfolio input data */
  val INPUT_PORTFS = "portfs.txt"

  /** Create DB Connection which is the IP address of DB server and database name */
  val mongodb = MongoConnection("127.0.0.1")("parabond")

  def main(args: Array[String]): Unit = {
    loadBonds
  }
  
  /** Load the bonds */
  def loadBonds() = {

    // Connects to Bonds collection
    val mongo = mongodb(COLL_BONDS)

    // Dropping the collection to recreate with fresh data
    mongo.drop()
    
    // Loop through input data file, convert each record to a mongo object,
    // and store in mongo
    for (record <- Source.fromFile(INPUT_BONDS).getLines()) {      
      // Get the record details of a bond
      val details = record.split(",");
      
      val id = details(0).trim.toInt
      
      val entry = MongoDbObject("id" -> id, "coupon" -> details(1).trim.toDouble, "freq" -> details(2).trim.toInt, "tenor" -> details(3).trim.toInt, "maturity" -> details(4).trim.toDouble)
      
      mongo.insertOne(entry)
      
      println("loaded bond: "+id)      
    }
    
    // Print the current size of Bond Collection in DB
    println(mongo.count + " documents inserted into collection: "+COLL_BONDS)    
  }
}