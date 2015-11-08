package parabond.mongo

import com.mongodb.MongoClient


/**
 * @author Ron.Coleman
 */

object MongoConnection {
  def apply(host: String) = new MongoConnection(new MongoClient(host))
}

class MongoConnection(client: MongoClient) {
  def apply(coll: String): MongoDb = new MongoDb(client.getDatabase(coll))
}