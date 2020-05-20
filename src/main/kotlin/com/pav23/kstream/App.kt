package com.pav23.kstream

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.KStream
import java.util.*

fun main(args: Array<String>) {

    val config = Properties()
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "capitaliser")
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)

    runTopology(capitaliseTopology(), config)

}


fun runTopology(topology: Topology, config: Properties) {
    val kafkaStream = KafkaStreams(topology, config)
    kafkaStream.start()
}


fun capitaliseTopology(): Topology {
    val streamsBuilder = StreamsBuilder()

    streamsBuilder.stream<String, String>("lowercase.topic")
            .mapValues { v ->  v.toUpperCase(Locale.UK)}
            .to("uppercase.topic")

    return streamsBuilder.build()
}