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
package parabond.db

import scala.io.Source

import parabond.casa.MongoDbObject
import parabond.casa.MongoHelper
import parabond.util.Constant

/**
 * This object load bonds and portfolios into the databasea. The bond parameters and ids
 * are pre-generated and stored in the files, bonds.txt, and portfs.txt.
 */
object DbLoader {
  /** Create DB Connection which is the IP address of DB server and database name */
  //val mongodb = MongoConnection("127.0.0.1")("parabond")
  val mongodb = parabond.casa.MongoConnection(MongoHelper.host)("parabond") 

  /** Load bonds and portfolios */
  def main(args: Array[String]): Unit = {
    loadBonds
    loadPortfolios
  }
  
  /** Loads the bonds into the database from INPUT_BONDS. */
  def loadBonds: Unit = {

    // Connects to Bonds collection
    val mongo = mongodb(Constant.COLL_BONDS_NAME)

    // Dropping the collection to recreate with fresh data
    mongo.drop()
    
    // Loop through input data file, convert each record to a mongo object,
    // and store in mongo
    for (record <- Source.fromFile(Constant.INPUT_BONDS_FILENAME).getLines()) {      
      // Get the record details of a bond
      val details = record.split(",");
      
      val id = details(0).trim.toInt
      
      val entry = MongoDbObject("id" -> id, "coupon" -> details(1).trim.toDouble, "freq" -> details(2).trim.toInt, "tenor" -> details(3).trim.toInt, "maturity" -> details(4).trim.toDouble)
      
      mongo.insertOne(entry)
      
      println("loaded bond: "+id)      
    }
    
    // Print the current size of Bond Collection in DB
    println(mongo.count + " documents inserted into collection: "+Constant.COLL_BONDS_NAME)    
  }
  
  /** Loads portfolios into the database from the INPUT_PORTFS file */
  def loadPortfolios() = {

    // Connects to Portfolio collection
    val mongo = mongodb(Constant.COLL_PORTFOLIOS_NAME)

    // Dropping it to recreate with fresh data
    mongo.drop()

    // Loop through input data file, convert each record to a mongo object,
    // and store in mongo
    for (record <- Source.fromFile(Constant.INPUT_PORTFS_FILENAME).getLines()) {          
      val ids = record.split(" +").toList    
      
      val bondsIds = for (id <- ids) yield id.trim.toInt

      val portfId = ids(0).trim.toInt
      
      val entry = MongoDbObject("id" -> portfId, "instruments" -> bondsIds.drop(2))
      
      mongo.insertOne(entry)
      
      println("loaded portfolio: "+portfId)       
    }

    // Print the current size of Bond Collection in DB
    println(mongo.count + " documents inserted into collection: "+Constant.COLL_PORTFOLIOS_NAME)

  }
}