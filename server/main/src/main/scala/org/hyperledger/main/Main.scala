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
package org.hyperledger.main

import java.security.Security

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.hyperledger.connector.InMemoryConnectorFactory
import org.hyperledger.jms.JMSConnectorFactory
import org.hyperledger.network.HyperLedgerExtension

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source

object Main extends App {
  Security.addProvider(new BouncyCastleProvider)

  if (System.getProperty("logback.configurationFile") == null)
    System.setProperty("logback.configurationFile", "conf/logback.xml")

  val configFile = if (args.length >= 1) args(0)
  else System.getProperty("hyperledger.configurationFile", "conf/application.conf")

  def createAndStartServer(system: ActorSystem) = {
    val hyperLedger = HyperLedgerExtension(system)
    system.log.info("Starting HyperLedger core services")

    val coreAssembly = hyperLedger.coreAssembly
    val jmsConnector =
      if (JMSConnectorFactory.isEnabled(system.settings.config)) JMSConnectorFactory.fromConfig(system.settings.config)
      else new InMemoryConnectorFactory

    val bcsapiServer = new BCSAPIServer(coreAssembly, jmsConnector)

    system.log.info("Starting HyperLedger connector")
    bcsapiServer.init()

    system.registerOnTermination {
      system.log.info("Shutting down HyperLedger connector")
      bcsapiServer.destroy()
    }
    sys.addShutdownHook {
      //      system.log.info("Shutting down HyperLedger connector")
      //      bcsapiServer.destroy()

      system.log.info("Shutting down HyperLedger Core Services")
      val terminate = system.terminate()
      Await.ready(terminate, 60.seconds)
    }

    bcsapiServer
  }

  val version = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("version.txt")).mkString

  val config = ConfigFactory.load(ConfigFactory.parseReader(Source.fromFile(configFile).reader()))

  implicit val system = ActorSystem("hyperledger", config)

  createAndStartServer(system)
}
