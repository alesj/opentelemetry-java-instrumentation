/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients;

import io.opentelemetry.api.trace.Span;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface SpanDecorator {

  /** Method called before record is sent by producer. */
  <K, V> void onSend(ProducerRecord<K, V> record, Span span);

  /** Method called when record is received in consumer. */
  <K, V> void onResponse(ConsumerRecord<K, V> record, Span span);

  /** Method called when an error occurs. */
  <K, V> void onError(Exception exception, Span span);

  /** Gives a SpanDecorator with the standard tags. */
  SpanDecorator STANDARD_TAGS = new StandardSpanDecorator();
}
