/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients

import io.opentelemetry.instrumentation.test.InstrumentationSpecification
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.Assert
import org.testcontainers.containers.KafkaContainer
import spock.lang.Shared

import java.time.Duration
import java.util.concurrent.TimeUnit

abstract class KafkaClientBaseTest extends InstrumentationSpecification {

  protected static final SHARED_TOPIC = "shared.topic"

  @Shared
  static KafkaContainer kafka
  @Shared
  static Producer<Integer, String> producer
  @Shared
  static Consumer<Integer, String> consumer

  static TopicPartition topicPartition = new TopicPartition(SHARED_TOPIC, 0)

  def setupSpec() {
    kafka = new KafkaContainer()
    kafka.start()

    // create test topic
    AdminClient.create(["bootstrap.servers": kafka.bootstrapServers]).withCloseable { admin ->
      admin.createTopics([new NewTopic(SHARED_TOPIC, 1, (short) 1)]).all().get(10, TimeUnit.SECONDS)
    }

    producer = new KafkaProducer<>(producerProps())

    consumer = new KafkaConsumer<>(consumerProps())

    // assign only existing topic partition
    consumer.assign([topicPartition])
    consumer.seekToBeginning([topicPartition])
  }

  Iterable<ConsumerRecord<Integer, String>> records(int expected) {
    int n = 0
    while (n++ < 600) {
      def records = consumer.poll(Duration.ofSeconds(5).toMillis())
      // not the best, as we could probably get records in different polls,
      // but collecting all records in a list breaks propagation tests
      if (records.count() >= expected) {
        return records
      }
      sleep(100)
    }
    Assert.fail("Missing " + expected + " records")
  }

  Map<String, ?> producerProps() {
    // values copied from spring's KafkaTestUtils
    return [
      "bootstrap.servers": kafka.bootstrapServers,
      "retries"          : 0,
      "batch.size"       : "16384",
      "linger.ms"        : 1,
      "buffer.memory"    : "33554432",
      "key.serializer"   : IntegerSerializer,
      "value.serializer" : StringSerializer
    ]
  }

  Map<String, ?> consumerProps() {
    // values copied from spring's KafkaTestUtils
    return [
      "bootstrap.servers"      : kafka.bootstrapServers,
      "group.id"               : "test",
      "enable.auto.commit"     : "false",
      "auto.commit.interval.ms": "10",
      "session.timeout.ms"     : "60000",
      "key.deserializer"       : IntegerDeserializer,
      "value.deserializer"     : StringDeserializer
    ]
  }

  def cleanupSpec() {
    consumer?.close()
    producer?.close()
    kafka.stop()
  }
}