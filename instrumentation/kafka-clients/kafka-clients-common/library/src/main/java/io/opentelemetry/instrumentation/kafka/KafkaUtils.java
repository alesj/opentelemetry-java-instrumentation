/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafka;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.SpanKindExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.SpanNameExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingSpanNameExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class KafkaUtils {

  public static Instrumenter<ProducerRecord<?, ?>, Void> buildProducerInstrumenter(
      String instrumentationName) {
    KafkaProducerAttributesExtractor attributesExtractor = new KafkaProducerAttributesExtractor();
    SpanNameExtractor<ProducerRecord<?, ?>> spanNameExtractor =
        MessagingSpanNameExtractor.create(attributesExtractor);

    return Instrumenter.<ProducerRecord<?, ?>, Void>newBuilder(
            GlobalOpenTelemetry.get(), instrumentationName, spanNameExtractor)
        .addAttributesExtractor(attributesExtractor)
        .addAttributesExtractor(new KafkaProducerAdditionalAttributesExtractor())
        .newInstrumenter(SpanKindExtractor.alwaysProducer());
  }

  public static Instrumenter<ConsumerRecord<?, ?>, Void> buildConsumerInstrumenter(
      String instrumentationName) {
    KafkaConsumerAttributesExtractor attributesExtractor =
        new KafkaConsumerAttributesExtractor(MessageOperation.PROCESS);
    SpanNameExtractor<ConsumerRecord<?, ?>> spanNameExtractor =
        MessagingSpanNameExtractor.create(attributesExtractor);

    InstrumenterBuilder<ConsumerRecord<?, ?>, Void> builder =
        Instrumenter.<ConsumerRecord<?, ?>, Void>newBuilder(
                GlobalOpenTelemetry.get(), instrumentationName, spanNameExtractor)
            .addAttributesExtractor(attributesExtractor)
            .addAttributesExtractor(new KafkaConsumerAdditionalAttributesExtractor());
    if (KafkaConsumerExperimentalAttributesExtractor.isEnabled()) {
      builder.addAttributesExtractor(new KafkaConsumerExperimentalAttributesExtractor());
    }
    return KafkaPropagation.isPropagationEnabled()
        ? builder.newConsumerInstrumenter(new KafkaConsumerRecordGetter())
        : builder.newInstrumenter(SpanKindExtractor.alwaysConsumer());
  }

  private KafkaUtils() {}
}
