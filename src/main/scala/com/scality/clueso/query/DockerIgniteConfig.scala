package com.scality.clueso.query

import java.net._
import java.util
import java.util.Collections

import com.google.common.io.BaseEncoding
import org.apache.ignite.configuration.{ConnectorConfiguration, IgniteConfiguration}
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
/**
  * Create a Docker Swarm compatible configuration
  */
object DockerIgniteConfig {
  private val LOG = Logger.getLogger(classOf[DockerIgniteConfig])
  private val DISCOVERY_PORT = 47100

  /**
    * Gets a list of all network addresses (on all interfaces)
    * For each interface the associated addresses are consistently sorted in ascending order
    */
  private def getNetworkAddresses = try {
    val hex = BaseEncoding.base16
    val all = new util.ArrayList[String]
    import scala.collection.JavaConversions._
    for (itf <- Collections.list(NetworkInterface.getNetworkInterfaces)) {



      all.addAll(itf.getInetAddresses()
          .filter((x: InetAddress) => x.isInstanceOf[Inet4Address])
          .map(_.getAddress)
          .map(hex.encode)
          .toList
          .sorted.map(hex.decode(_))
          .map((b: Array[Byte]) =>
            s"${0xff & b(0)}.${0xff & b(1)}.${0xff & b(2)}.${0xff & b(3)}")
          .toList)
    }
    LOG.trace("Available network addresses: " + all)
    all
  } catch {
    case e: SocketException =>
      throw new RuntimeException("Failed to load network interfaces", e)
  }
}

class DockerIgniteConfig() extends IgniteConfiguration {
  val communicationSpi = new TcpCommunicationSpi
  val discoverySpi = new TcpDiscoverySpi
  // single port for discovery
  discoverySpi.setLocalPort(DockerIgniteConfig.DISCOVERY_PORT)
  discoverySpi.setLocalPortRange(0)
  // disable binding port for shared memory communication (used only for same host)
  communicationSpi.setSharedMemoryPort(-1)
  applyLocalAddress(discoverySpi, communicationSpi)
  applyDiscoveryDns(discoverySpi, communicationSpi)
  setCommunicationSpi(communicationSpi)
  setDiscoverySpi(discoverySpi)
  val connectorConfig = new ConnectorConfiguration
  // this timeout applies for memcached clients as well!
  connectorConfig.setIdleTimeout(60 * 1000) // connection will timeout after 1 minute

  setConnectorConfiguration(connectorConfig)
  if (System.getProperties.containsKey("METRICS")) setMetricsLogFrequency(10 * 60 * 1000) // log metrics every 10 minutes
  else {
    DockerIgniteConfig.LOG.debug("Metrics disabled")
    setMetricsLogFrequency(0)
  }

  private def applyDiscoveryDns(discoverySpi: TcpDiscoverySpi, communicationSpi: TcpCommunicationSpi): Unit = { // Load the dns address, probably tasks.servname which resolves to the list of host addresses
    val dns = System.getenv("CLUSTER_DNS")
    if (dns != null) try {

      val hosts = InetAddress.getAllByName(dns)
        .map((x: InetAddress) => x.getHostAddress + ":" + DockerIgniteConfig.DISCOVERY_PORT)
        .toList
      DockerIgniteConfig.LOG.info("Set HOSTS=" + hosts)
      val ipFinder = new TcpDiscoveryVmIpFinder(true)
      ipFinder.setAddresses(hosts)
      discoverySpi.setIpFinder(ipFinder)
    } catch {
      case e: UnknownHostException =>
        throw new RuntimeException("Failed to resolve DNS '" + dns + "'", e)
    }
    else {
      DockerIgniteConfig.LOG.warn("No -DCLUSTER_DNS is set, so multicast will be used instead")
      discoverySpi.setIpFinder(new TcpDiscoveryMulticastIpFinder)
    }
  }

  private def applyLocalAddress(discoverySpi: TcpDiscoverySpi, communicationSpi: TcpCommunicationSpi): Unit = { // Find a matching local address to apply
    val network = System.getenv("NETWORK_PREFIX")
    if (network != null) {
      val all = DockerIgniteConfig.getNetworkAddresses
      var matches = all.filter((x: String) => x.startsWith(network))

      matches = matches.reverse
      if (matches.isEmpty) throw new RuntimeException("No matching addresses found for NETWORK_PREFIX=" + network + ", ADDRS=" + all)
      else {
        DockerIgniteConfig.LOG.debug("Picking first of matching addresses: " + matches)
        val addr = matches(0)
        DockerIgniteConfig.LOG.info("Set LOCAL_ADDRESS=" + addr)
        communicationSpi.setLocalAddress(addr)
        discoverySpi.setLocalAddress(addr)
      }
    }
    else DockerIgniteConfig.LOG.warn("No -DNETWORK_PREFIX is set, so localAddress will be auto-discovered")
  }
}