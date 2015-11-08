package parabond.mongo

import org.bson.Document
import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * @author Ron.Coleman
 */
object MongoDbObject { 
  def apply(args: (String,Any)*): Document = {
    println("in apply")
    val doc = new Document
    
    for(arg <- args) {
      val (key, value) = arg
      
      value match {
        case list: List[_] =>
          import scala.collection.JavaConverters._
          
          val ls = value.asInstanceOf[List[Int]].asJava
          
          doc.append(key, ls)
        case _ =>
          doc.append(key,value)
      }
    }
    doc
  }
}