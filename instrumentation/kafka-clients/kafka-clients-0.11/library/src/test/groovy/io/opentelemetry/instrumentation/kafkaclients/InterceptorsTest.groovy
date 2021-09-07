/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients


import io.opentelemetry.instrumentation.test.InstrumentationSpecification
import io.opentelemetry.instrumentation.test.LibraryTestTrait
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.junit.Rule
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.MessageListener
import org.springframework.kafka.test.rule.EmbeddedKafkaRule
import org.springframework.kafka.test.utils.ContainerTestUtils
import org.springframework.kafka.test.utils.KafkaTestUtils
import spock.lang.Unroll

import java.nio.charset.StandardCharsets
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

class InterceptorsTest extends InstrumentationSpecification implements LibraryTestTrait {

  private static final TOPIC = "xtopic"

  @Rule
  EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, TOPIC)

  @Unroll
  def "test interceptors"() throws Exception {
    setup:
    def senderProps = KafkaTestUtils.producerProps(
      embeddedKafka.getEmbeddedKafka().getBrokersAsString())
    senderProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingProducerInterceptor.getName())

    def producerFactory = new DefaultKafkaProducerFactory<Integer, String>(senderProps)
    def kafkaTemplate = new KafkaTemplate<Integer, String>(producerFactory)

    // set up the Kafka consumer properties
    def consumerProperties = KafkaTestUtils.consumerProps("xgroup", "true", embeddedKafka.getEmbeddedKafka())
    consumerProperties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingConsumerInterceptor.getName())

    // create a Kafka consumer factory
    def consumerFactory = new DefaultKafkaConsumerFactory<Integer, String>(consumerProperties)

    // set the topic that needs to be consumed
    def containerProperties = new ContainerProperties(TOPIC)

    // create a Kafka MessageListenerContainer
    def container = new KafkaMessageListenerContainer<Integer, String>(consumerFactory, containerProperties)

    // create a thread safe queue to store the received message
    def records = new LinkedBlockingQueue<ConsumerRecord<Integer, String>>()

    // setup a Kafka message listener
    container.setupMessageListener(new MessageListener<Integer, String>() {
      @Override
      void onMessage(ConsumerRecord<Integer, String> record) {
        records.add(record)
      }
    })

    // start the container and underlying message listener
    container.start()

    // wait until the container has the required number of assigned partitions
    ContainerTestUtils.waitForAssignment(container, embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic())

    when:
    String message = "Testing 123"
    def produced = kafkaTemplate.send(TOPIC, message).get().producerRecord

    then:
    // check that the message was received
    def received = records.poll(5, TimeUnit.SECONDS)

    // TODO -- add asserts ...
    println "XXX------------------------------"
    println "produced = " + printHeader(produced.headers())
    println "received = " + printHeader(received.headers())

    cleanup:
    producerFactory.destroy()
    container?.stop()
  }

  def printHeader(Headers headers) {
    def output = new StringBuilder()
    headers.forEach(new Consumer<Header>() {
      @Override
      void accept(Header header) {
        output
          .append(header.key())
          .append("=")
          .append(new String(header.value(), StandardCharsets.UTF_8))
          .append(",")
      }
    })
    return output
  }
}

