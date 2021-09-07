/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafka;

import static io.opentelemetry.instrumentation.kafka.KafkaUtils.buildConsumerInstrumenter;
import static io.opentelemetry.instrumentation.kafka.KafkaUtils.buildProducerInstrumenter;

import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class KafkaSingletons {
  private static final String INSTRUMENTATION_NAME = "io.opentelemetry.kafka-clients-0.11";

  private static final Instrumenter<ProducerRecord<?, ?>, Void> PRODUCER_INSTRUMENTER =
      buildProducerInstrumenter(INSTRUMENTATION_NAME);
  private static final Instrumenter<ConsumerRecord<?, ?>, Void> CONSUMER_INSTRUMENTER =
      buildConsumerInstrumenter(INSTRUMENTATION_NAME);

  public static Instrumenter<ProducerRecord<?, ?>, Void> producerInstrumenter() {
    return PRODUCER_INSTRUMENTER;
  }

  public static Instrumenter<ConsumerRecord<?, ?>, Void> consumerInstrumenter() {
    return CONSUMER_INSTRUMENTER;
  }

  private KafkaSingletons() {}
}
