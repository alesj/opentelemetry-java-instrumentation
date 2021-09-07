/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

// Includes work from:
/*
 * Copyright 2017-2021 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.opentelemetry.instrumentation.kafkaclients;

import java.util.function.BiFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Returns a string to be used as the name of the spans, based on the operation preformed and the
 * record the span is based off of.
 */
public class ClientSpanNameProvider {

  // Operation Name as Span Name
  public static BiFunction<String, ConsumerRecord, String> consumerOperationName =
      (operationName, consumerRecord) -> replaceIfNull(operationName);

  public static BiFunction<String, ProducerRecord, String> producerOperationName =
      (operationName, producerRecord) -> replaceIfNull(operationName);

  private static String replaceIfNull(String input) {
    return (input == null) ? "unknown" : input;
  }
}
