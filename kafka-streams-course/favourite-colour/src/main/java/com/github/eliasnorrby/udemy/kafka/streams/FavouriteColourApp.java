package com.github.eliasnorrby.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class FavouriteColourApp {

//  public static String[] allowedColours = new String[] { "red", "green", "blue" };
  public static ArrayList<String> allowedColours = new ArrayList<>(Arrays.asList("red", "green", "blue"));

  public static void main(String[] args) {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> colourInput = builder.stream("favourite-colour-input");

    KStream<String, String> favUpdates = colourInput
      .mapValues(pair -> pair.toLowerCase())
      .mapValues(pair -> Arrays.asList(pair.split(",")))
      // Basic validation
      .filter((key, pair) -> pair.size() == 2)
      // Set user as key
      .selectKey((key, pair) -> pair.get(0))
      // Set colour as value
      .mapValues(pair -> pair.get(1));

    KStream<String, String> favInput = favUpdates.through("favourite-colour-updates");

    KTable<String, Long> colourCounts = favInput
      .selectKey((ignoredKey, colour) -> colour)
      .filter((key, colour) -> allowedColours.contains(colour))
      .groupByKey()
      .count();

    colourCounts.toStream().to("favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    streams.start();

    // print the topology
    System.out.println(streams.toString());

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


//    KTable<String, Long> colourCounts = colourInput
//      .mapValues(pair -> pair.toLowerCase())
//      .mapValues(pair -> Arrays.asList(pair.split(",")))
//      .filter((key, pair) -> pair.size() == 2)
//      .selectKey((key, pair) -> pair.get(0))
//      .mapValues(pair -> pair.get(1))
//      .filter((key, pair) -> allowedColours.contains(pair))
//      .groupByKey()
//      .count();
  }

}
