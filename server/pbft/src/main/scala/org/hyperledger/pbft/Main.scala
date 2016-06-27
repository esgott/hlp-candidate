/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hyperledger.pbft

import java.security.Security

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.bouncycastle.jce.provider.BouncyCastleProvider

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source

object Main extends App {
  Security.addProvider(new BouncyCastleProvider)

  if (System.getProperty("logback.configurationFile") == null)
    System.setProperty("logback.configurationFile", "conf/logback.xml")

  val configFile = if (args.length >= 1) args(0)
  else System.getProperty("hyperledger.configurationFile", "conf/application.conf")

  val config = ConfigFactory.load(ConfigFactory.parseReader(Source.fromFile(configFile).reader))
  implicit val actorSystem: ActorSystem = ActorSystem("hyperledger-pbft", config)

  val ext = PbftExtension(actorSystem)
  val server = actorSystem.actorOf(PbftServer.props(ext.settings.bindAddress))

  actorSystem.registerOnTermination {
    //bcsapiServer.destroy()
  }

  sys.addShutdownHook {
    val termination = actorSystem.terminate()
    Await.ready(termination, 60 seconds)
  }

}
