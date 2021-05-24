/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.lettuce.v4_0;

import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.protocol.RedisCommand;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.SpanKindExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.SpanNameExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.db.DbAttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.db.DbSpanNameExtractor;

public final class LettuceInstrumenters {
  private static final String INSTRUMENTATION_NAME = "io.opentelemetry.javaagent.jedis-1.4";

  private static final Instrumenter<RedisCommand<?, ?, ?>, Void> INSTRUMENTER;

  private static final Instrumenter<RedisURI, Void> CONNECT_INSTRUMENTER;

  static {
    DbAttributesExtractor<RedisCommand<?, ?, ?>> attributesExtractor =
        new LettuceDbAttributesExtractor();
    SpanNameExtractor<RedisCommand<?, ?, ?>> spanName =
        DbSpanNameExtractor.create(attributesExtractor);

    INSTRUMENTER =
        Instrumenter.<RedisCommand<?, ?, ?>, Void>newBuilder(
                GlobalOpenTelemetry.get(), INSTRUMENTATION_NAME, spanName)
            .addAttributesExtractor(attributesExtractor)
            .newInstrumenter(SpanKindExtractor.alwaysClient());

    CONNECT_INSTRUMENTER =
        Instrumenter.<RedisURI, Void>newBuilder(
                GlobalOpenTelemetry.get(), INSTRUMENTATION_NAME, redisUri -> "CONNECT")
            .addAttributesExtractor(new LettuceNetAttributesExtractor())
            .addAttributesExtractor(new LettuceConnectAttributesExtractor())
            .newInstrumenter(SpanKindExtractor.alwaysClient());
  }

  public static Instrumenter<RedisCommand<?, ?, ?>, Void> instrumenter() {
    return INSTRUMENTER;
  }

  public static Instrumenter<RedisURI, Void> connectInstrumenter() {
    return CONNECT_INSTRUMENTER;
  }

  private LettuceInstrumenters() {}
}