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
package parabond.casa

import com.mongodb.MongoClient
import java.util.logging.Level
import java.util.logging.Logger


/**
 * This class allows one to connect to mongodb.
 * @author Ron Coleman
 */
object MongoConnection {
  // Suppress mongo diagnostics
  // See http://stackoverflow.com/questions/29454916/how-to-prevent-logging-on-console-when-connected-to-mongodb-from-java
  // Code appears to be here http://apiwave.com/java/api/org.mongodb.diagnostics.logging.JULLogger
  Logger.getLogger( "org.mongodb.driver" ).setLevel(Level.SEVERE)

  def apply(host: String) = new MongoConnection(new MongoClient(host))
}


/**
 * This class implements the mongodb connection facade.
 * @author Ron.Coleman
 */
class MongoConnection(client: MongoClient) {
  def apply(coll: String): MongoDb = new MongoDb(client.getDatabase(coll))
}