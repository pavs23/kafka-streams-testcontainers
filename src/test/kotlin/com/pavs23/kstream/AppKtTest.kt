package com.pav23.kstream

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Before
import org.junit.Test
import org.testcontainers.containers.KafkaContainer
import java.util.*

/**
 * Created by pavan on 20/5/20.
 */
class AppKtTest {

    val kafkaContainer = KafkaContainer()

    @Before
    fun setUp() {
        kafkaContainer.start()
    }

    /*
        I've followed the tutorial posted here: https://dev.to/itnext/using-docker-to-test-your-kafka-applications-fmm

        This tutorial uses the TopologyTestDriver which doesn't use a real kafka broker even though we provide it with a bootstrap server URL.

        This test also passes when we use "localhost:12345" which definitely doesn't have a broker.

    */

    @Test
    fun `should output capitalised data to output topic`() {

        val topology = capitaliseTopology()

        val config = Properties()
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "capitaliser")
//        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.bootstrapServers)
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:12345")
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)

        val topologyTestDriver = TopologyTestDriver(topology, config)

        val inputTopic = topologyTestDriver.createInputTopic("lowercase.topic", Serdes.String().serializer(), Serdes.String().serializer())
        val outputTopic = topologyTestDriver.createOutputTopic("uppercase.topic", Serdes.String().deserializer(), Serdes.String().deserializer())

        inputTopic.pipeInput("hello-world-key-1", "hello-world-value-1")
        inputTopic.pipeInput("hello-world-key-2", "hello-world-value-2")
        inputTopic.pipeInput("hello-world-key-3", "hello-world-value-3")
        inputTopic.pipeInput("hello-world-key-4", "hello-world-value-4")
        inputTopic.pipeInput("hello-world-key-5", "hello-world-value-5")

        val output = outputTopic.readValuesToList()

        println(output.toString())

    }

    /*
        In this test we produce to the input topic "lowercase.topic" using the Kafka Producer.

        If the topology runs, we should see something in the "uppercase.topic".

        My method of verification is by adding a breakpoint and then `docker exec -it <container-id> bash`
            then use the kafka-console-consumer to check the contents of the "uppercase.topic" from beginning.

            Using this method we can see "lowercase.topic" does have data in it but there's nothing in the "uppercase.topic"

     */

    @Test
    fun `attempt to run a real topology`() {
        val topology = capitaliseTopology()

        val config = Properties()

        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "capitaliser")
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.bootstrapServers)
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        config.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 1);
        config.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "0");

        val kafkaAdminClient = AdminClient.create(config)

        kafkaAdminClient.createAcls(listOf(
                AclBinding(ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
                        AccessControlEntry("cap", kafkaContainer.bootstrapServers, AclOperation.ALL, AclPermissionType.ALLOW))
                ))

        kafkaAdminClient.createTopics(listOf(NewTopic("lowercase.topic", 1, 1), NewTopic("uppercase.topic", 1, 1)))

        runTopology(topology, config)

        val kafkaProducer = KafkaProducer<String, String>(config, Serdes.String().serializer(), Serdes.String().serializer())

        val recordMetadata = kafkaProducer.send(ProducerRecord("lowercase.topic", "hello-world-key-1", "hello-world-value-1")).get()

        println(recordMetadata)

    }

}