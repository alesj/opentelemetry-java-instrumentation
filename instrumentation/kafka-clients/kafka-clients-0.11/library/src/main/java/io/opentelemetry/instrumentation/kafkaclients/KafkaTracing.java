/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.kafka.KafkaHeadersGetter;
import io.opentelemetry.instrumentation.kafka.KafkaHeadersSetter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTracing {
  public static final String INSTRUMENTATION_NAME = "io.opentelemetry.kafka-clients-0.11.library";

  private static final Logger logger = LoggerFactory.getLogger(KafkaTracing.class);

  private static final TextMapGetter<Headers> GETTER = new KafkaHeadersGetter();

  private static final TextMapSetter<Headers> SETTER = new KafkaHeadersSetter();

  private final Instrumenter<ProducerRecord<?, ?>, Void> producerInstrumenter;
  private final Instrumenter<ConsumerRecord<?, ?>, Void> consumerProcessInstrumenter;

  KafkaTracing(
      Instrumenter<ProducerRecord<?, ?>, Void> producerInstrumenter,
      Instrumenter<ConsumerRecord<?, ?>, Void> consumerProcessInstrumenter) {
    this.producerInstrumenter = producerInstrumenter;
    this.consumerProcessInstrumenter = consumerProcessInstrumenter;
  }

  public static KafkaTracingBuilder create(OpenTelemetry openTelemetry) {
    return new KafkaTracingBuilder(openTelemetry);
  }

  private static TextMapPropagator propagator() {
    return GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
  }

  /**
   * Build and inject span into record. Return Runnable handle to end the current span.
   *
   * @param record the producer record to inject span info.
   * @return runnable to close the current span
   */
  <K, V> Runnable buildAndInjectSpan(ProducerRecord<K, V> record) {
    Context currentContext = Context.current();

    if (!producerInstrumenter.shouldStart(currentContext, record)) {
      return () -> {};
    }

    Context current = producerInstrumenter.start(currentContext, record);
    try (Scope ignored = current.makeCurrent()) {
      try {
        propagator().inject(current, record.headers(), SETTER);
      } catch (Throwable t) {
        // it can happen if headers are read only (when record is sent second time)
        logger.error("failed to inject span context. sending record second time?", t);
      }
    }

    return () -> producerInstrumenter.end(current, record, null, null);
  }

  <K, V> void buildAndFinishSpan(ConsumerRecords<K, V> records) {
    Context currentContext = Context.current();
    for (ConsumerRecord<K, V> record : records) {
      Context linkedContext = propagator().extract(currentContext, record.headers(), GETTER);
      currentContext.with(Span.fromContext(linkedContext));

      if (!consumerProcessInstrumenter.shouldStart(currentContext, record)) {
        continue;
      }

      Context current = consumerProcessInstrumenter.start(currentContext, record);
      consumerProcessInstrumenter.end(current, record, null, null);
    }
  }
}
