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
package org.hyperledger.network

import java.net.{InetAddress, InetSocketAddress}

import com.typesafe.config.Config
import com.typesafe.config.ConfigException.BadValue

import scala.collection.JavaConverters._

case class FixedAddressDiscovery(addresses: List[InetSocketAddress])

object HyperLedgerExtensionSettings {
  def apply(c: Config) = new HyperLedgerExtensionSettings(c)
}

class HyperLedgerExtensionSettings(fullConfig: Config) {
  val config = fullConfig.getConfig("hyperledger")

  val dispatcherName = "hyperledger.network.dispatcher"

  val chain = config.getString("blockchain.chain") match {
    case "testnet3" => Testnet3BitcoinNetwork
    case "regtest" => RegtestBitcoinNetwork
    case "privateregtest" => RegtestBitcoinNetwork
    case "production" => ProductionBitcoinNetwork
    case "signedregtest" => RegtestBitcoinNetwork
    case c => throw new BadValue("hyperledger.chain", s"Invalid blockchain configuration $c")
  }

  val outgoingConnections = config.getInt("network.outgoingConnections")

  val bindAddress = if (config.hasPath("network.bindAddress")) {
    val port = if (config.hasPath("network.bindPort")) config.getInt("network.bindPort") else chain.defaultPort
    Some(new InetSocketAddress(InetAddress.getByName(config.getString("network.bindAddress")), port))
  } else None

  // TODO if we don't specify bindAddress, we still have to add something to the VersionMessage. What would be correct value here?
  def localAddress = bindAddress.getOrElse(InetSocketAddress.createUnresolved("127.0.0.1", chain.defaultPort))

  val discoverySettings = config.getConfigList("network.discovery").asScala.map { discoveryConf =>
    discoveryConf.getString("type") match {
      case "fixed" =>
        val peers = discoveryConf.getStringList("peers").asScala.map { peer =>
          val (host, port) = peer.splitAt(peer.indexOf(':'))
          new InetSocketAddress(InetAddress.getByName(host), Integer.valueOf(port.tail))
        }
        FixedAddressDiscovery(peers.toList)
    }
  }.toList

  val blockSignature = {
    val enabled = config.getBoolean("blockSignature.enabled")
  }
}
