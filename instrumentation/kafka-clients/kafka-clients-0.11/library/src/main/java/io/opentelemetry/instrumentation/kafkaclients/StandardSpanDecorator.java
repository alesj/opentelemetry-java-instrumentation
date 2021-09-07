/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

class StandardSpanDecorator implements SpanDecorator {

  static final String COMPONENT_NAME = "java-kafka";
  static final String KAFKA_SERVICE = "kafka";

  @Override
  public <K, V> void onSend(ProducerRecord<K, V> record, Span span) {
    setCommonTags(span);
    span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, record.topic());
    if (record.partition() != null) {
      span.setAttribute("partition", record.partition());
    }
  }

  @Override
  public <K, V> void onResponse(ConsumerRecord<K, V> record, Span span) {
    setCommonTags(span);
    span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, record.topic());
    span.setAttribute("partition", record.partition());
    span.setAttribute("offset", record.offset());
  }

  @Override
  public void onError(Exception exception, Span span) {
    span.setAttribute(SemanticAttributes.EXCEPTION_ESCAPED, Boolean.TRUE);
    span.setAllAttributes(errorLogs(exception));
  }

  private static Attributes errorLogs(Throwable throwable) {
    AttributesBuilder errorLogs = Attributes.builder();
    errorLogs.put("event", SemanticAttributes.EXCEPTION_TYPE.getKey());
    errorLogs.put("error.kind", throwable.getClass().getName());
    errorLogs.put("error.object", throwable.toString());
    errorLogs.put("message", throwable.getMessage());

    StringWriter sw = new StringWriter();
    throwable.printStackTrace(new PrintWriter(sw));
    errorLogs.put("stack", sw.toString());

    return errorLogs.build();
  }

  private static void setCommonTags(Span span) {
    span.setAttribute(SemanticAttributes.PEER_SERVICE, KAFKA_SERVICE);
  }
}
