/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TracingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {
  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
    TracingKafkaUtils.buildAndInjectSpan(producerRecord).run();
    return producerRecord;
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {}

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> map) {}
}
