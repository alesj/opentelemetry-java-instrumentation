/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.opentelemetry.instrumentation.kafka.KafkaUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import io.opentelemetry.instrumentation.kafka.ReceivedRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaTracingBuilder {
  private final OpenTelemetry openTelemetry;
  private final List<AttributesExtractor<ProducerRecord<?, ?>, Void>> producerExtractors =
      new ArrayList<>();
  private final List<AttributesExtractor<ReceivedRecords, Void>> consumerReceiveExtractors =
      new ArrayList<>();
  private final List<AttributesExtractor<ConsumerRecord<?, ?>, Void>> consumerProcessExtractors =
      new ArrayList<>();

  KafkaTracingBuilder(OpenTelemetry openTelemetry) {
    this.openTelemetry = Objects.requireNonNull(openTelemetry);
  }

  public void addProducerExtractors(AttributesExtractor<ProducerRecord<?, ?>, Void> extractor) {
    producerExtractors.add(extractor);
  }

  public void addConsumerReceiveExtractors(
      AttributesExtractor<ReceivedRecords, Void> extractor) {
    consumerReceiveExtractors.add(extractor);
  }

  public void addConsumerProcessExtractors(
      AttributesExtractor<ConsumerRecord<?, ?>, Void> extractor) {
    consumerProcessExtractors.add(extractor);
  }

  @SuppressWarnings("unchecked")
  public KafkaTracing build() {
    return new KafkaTracing(
        KafkaUtils.buildProducerInstrumenter(
            KafkaTracing.INSTRUMENTATION_NAME,
            openTelemetry,
            producerExtractors.toArray(new AttributesExtractor[0])),
        KafkaUtils.buildConsumerReceiveInstrumenter(
            KafkaTracing.INSTRUMENTATION_NAME,
            openTelemetry,
            consumerReceiveExtractors.toArray(new AttributesExtractor[0])),
        KafkaUtils.buildConsumerOperationInstrumenter(
            KafkaTracing.INSTRUMENTATION_NAME,
            openTelemetry,
            MessageOperation.RECEIVE,
            consumerProcessExtractors.toArray(new AttributesExtractor[0])));
  }
}
