package parabond.casa

import com.mongodb.MongoClient
import com.mongodb.client.MongoDatabase

/**
 * @author Ron.Coleman
 */
class MongoDb(db: MongoDatabase) {
  def apply(collName: String) = db.getCollection(collName)
}

object MongoDb {
  def apply(client: MongoClient, dbName: String): MongoDatabase = client.getDatabase(dbName)
}