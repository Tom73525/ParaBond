package parabond.mongo

import org.bson.Document
import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * @author Ron.Coleman
 */
object MongoDbObject { 
  def apply(args: (String,Any)*): Document = {
    val doc = new Document
    
    for(arg <- args) {
      val (key, value) = arg
      
      import scala.collection.JavaConverters._
      value match {
        case list: List[_] =>
          
          val ls = value.asInstanceOf[List[Int]].asJava
          
          doc.append(key, ls)
          
        case array: Array[_] =>
          val ls = array.toList.asJava

          doc.append(key, ls)
          
        case _ =>
          doc.append(key,value)
      }
    }
    doc
  }
  
  implicit def arrayToList[A](a: Array[A]) = a.toList
}