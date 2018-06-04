package com.github.gorros

import java.io.IOException

import io.debezium.kafka.KafkaCluster
import io.debezium.util.Testing
import junit.framework.TestCase
import org.junit.{AfterClass, BeforeClass}

trait KafkaIntegrationTest extends TestCase {

    val dataDir = Testing.Files.createTestingDirectory("cluster")
    if (kafkaCluster != null) throw new IllegalStateException
    var kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2181, 9092)
    kafkaCluster.deleteDataUponShutdown(false)
    kafkaCluster.deleteDataPriorToStartup(true)

    @throws[Exception]
    def generateData()

    @BeforeClass
    @throws[Exception]
    override def setUp() {
        kafkaCluster = kafkaCluster.deleteDataPriorToStartup(true).addBrokers(1).startup()
        generateData()
    }

    @AfterClass
    @throws[IOException]
    override def tearDown() {
        if (kafkaCluster != null) {
            kafkaCluster.shutdown()
            kafkaCluster = null
            val delete = dataDir.delete()
            // If files are still locked and a test fails: delete on exit to allow subsequent test execution
            if (!delete) dataDir.deleteOnExit()
        }
    }

}
