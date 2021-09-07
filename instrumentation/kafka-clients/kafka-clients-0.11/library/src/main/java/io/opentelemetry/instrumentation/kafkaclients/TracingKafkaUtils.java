/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients;

import static io.opentelemetry.instrumentation.kafka.KafkaSingletons.consumerInstrumenter;
import static io.opentelemetry.instrumentation.kafka.KafkaSingletons.producerInstrumenter;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.instrumentation.kafka.KafkaHeadersGetter;
import io.opentelemetry.instrumentation.kafka.KafkaHeadersSetter;
import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingKafkaUtils {

  private static final Logger logger = LoggerFactory.getLogger(TracingKafkaUtils.class);

  private static final TextMapGetter<Headers> GETTER = new KafkaHeadersGetter();

  private static final TextMapSetter<Headers> SETTER = new KafkaHeadersSetter();

  private static TextMapPropagator propagator() {
    return GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
  }

  /**
   * Extract Context from record headers.
   *
   * @param headers record headers
   * @return context
   */
  public static Context extractContext(Headers headers) {
    return propagator().extract(Context.current(), headers, GETTER);
  }

  /**
   * Inject current Context to record headers.
   *
   * @param headers record headers
   */
  public static void inject(Headers headers) {
    inject(Context.current(), headers);
  }

  /**
   * Inject Context to record headers.
   *
   * @param context the context
   * @param headers record headers
   */
  public static void inject(Context context, Headers headers) {
    propagator().inject(context, headers, SETTER);
  }

  /**
   * Build and inject span into record. Return Runnable handle to end the current span.
   *
   * @param record the producer record to inject span info.
   * @return runnable to close the current span
   */
  public static <K, V> Runnable buildAndInjectSpan(ProducerRecord<K, V> record) {
    return buildAndInjectSpan(record, Collections.singletonList(SpanDecorator.STANDARD_TAGS));
  }

  public static <K, V> Runnable buildAndInjectSpan(
      ProducerRecord<K, V> record, Collection<SpanDecorator> spanDecorators) {

    Context spanContext = extractContext(record.headers());

    Context current = producerInstrumenter().start(spanContext, record);
    try (Scope ignored = current.makeCurrent()) {

      Span span = Span.fromContext(current);
      for (SpanDecorator decorator : spanDecorators) {
        decorator.onSend(record, span);
      }

      try {
        inject(record.headers());
      } catch (Throwable t) {
        // it can happen if headers are read only (when record is sent second time)
        logger.error("failed to inject span context. sending record second time?", t);
      }
    }

    return () -> producerInstrumenter().end(current, record, null, null);
  }

  public static <K, V> void buildAndFinishChildSpan(ConsumerRecord<K, V> record) {
    buildAndFinishChildSpan(record, Collections.singletonList(SpanDecorator.STANDARD_TAGS));
  }

  public static <K, V> void buildAndFinishChildSpan(
      ConsumerRecord<K, V> record, Collection<SpanDecorator> spanDecorators) {

    Context parentContext = extractContext(record.headers());
    Context current = consumerInstrumenter().start(parentContext, record);
    try (Scope ignored = current.makeCurrent()) {

      Span span = Span.fromContext(current);
      for (SpanDecorator decorator : spanDecorators) {
        decorator.onResponse(record, span);
      }
    } catch (RuntimeException e) {
      consumerInstrumenter().end(current, record, null, e);
      throw e;
    }
    consumerInstrumenter().end(current, record, null, null);

    // Inject created span context into record headers for extraction by client to continue span
    // chain
    inject(current, record.headers()); // TODO -- OK?
  }
}
