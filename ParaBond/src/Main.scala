

/**
 * @author Ron.Coleman
 */
import org.bson.Document
import com.mongodb.MongoClient
import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoDatabase
import parabond.casa.MongoHelper
import parabond.casa.MongoDbObject
import parabond.casa.MongoConnection

object Main {
  
  def main(args: Array[String]): Unit = {
      val mongo = MongoConnection( "localhost" )("parabond")
//      
//      val db = mongo.getDatabase("myDb")   //getDB( "mydb" );
//      
//      println("connected successfully")
//      
//      val coll = db.getCollection("Portfolios");

      val coll = mongo("Portfolios")

//      val doc = new Document("name", "MongoDB").append("type", "database").append("count", 99).append("info", new Document("x", 203).append("y", 102));
    
      import scala.collection.JavaConverters._
      val list = List(1,2,3,4)
      val doc = MongoDbObject("myid" -> 1, "ron" -> list)

      coll.insertOne(doc)
    
      println("inserted successfully")
      
      val query = MongoDbObject("myid" -> 1)
      
      val results = coll.find(query)
      
      val list2 = MongoHelper.asList(results,"ron")
      
      if(list2.length != 0) {
        println("query successful: "+list2)
        
      }
        

//    if (results.hasNext) {
//      println("query successful")

//      val list = results.next().get("ron")
//      
//      import java.util.List
//      list match {
//        case ls: java.util.List[_] => 
//          val l = ls.asInstanceOf[java.util.List[_]].asScala.toList
//          println(l)
//        case _ =>
//          
//          
//      }
//
//    }

      
//      val doc2 = new Document("count", 2)
//      
//      val results : MongoCursor[Document]= coll.find().iterator// .findOneAndDelete(doc2)
//      
//      val buffer = ListBuffer[Document]()
//      while(results.hasNext()) {
//        val doc = results.next
//        buffer.append(doc)
//      }
//
//      
//      coll.deleteOne(MongoDbObj("count" -> 2))
//      
//      println("delete successful")
  }
}

//object MongoConnection {
//  def apply(host: String) = new MongoConnection(new MongoClient(host))
//}
//
//class MongoConnection(client: MongoClient) {
//  def apply(coll: String): MongoDb = new MongoDb(client.getDatabase(coll))
//}
//
//class MongoDb(db: MongoDatabase) {
//  def apply(collName: String) = db.getCollection(collName)
//}
//
//object MongoDb {
//  def apply(client: MongoClient, dbName: String): MongoDatabase = client.getDatabase(dbName)
//}
//
//object MongoDbObj {
//  val foo = 5 -> "hello"
//  
//  def bar(args: (Any,Any)*): Unit = {
//    
//  }
//  
//  def apply(args: (String,Any)*): Document = {
//    val doc = new Document
//    for(arg <- args) {
//      val (key, value) = arg
//      
//      doc.append(key,value)
//    }
//    doc
//  }
//}

class Bar(foo: String)

object Bar {
  def apply(foo: String) = new Bar(foo)
}


