package parabond.mongo

import com.mongodb.MongoClient
import java.util.logging.Level
import java.util.logging.Logger

/**
 * @author Ron.Coleman
 */

object MongoConnection {
  // Suppress mongo diagnostics
  // See http://stackoverflow.com/questions/29454916/how-to-prevent-logging-on-console-when-connected-to-mongodb-from-java
  // Code appears to be here http://apiwave.com/java/api/org.mongodb.diagnostics.logging.JULLogger
  Logger.getLogger( "org.mongodb.driver" ).setLevel(Level.SEVERE)

  def apply(host: String) = new MongoConnection(new MongoClient(host))
}

class MongoConnection(client: MongoClient) {
  def apply(coll: String): MongoDb = new MongoDb(client.getDatabase(coll))
}