/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.kafka.ReceivedRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

class TracingConsumer<K, V> implements Consumer<K, V> {
  private final Consumer<K, V> consumer;
  private final Instrumenter<ReceivedRecords, Void> receiveInstrumenter;
  private final Instrumenter<ConsumerRecord<K, V>, Void> processInstrumenter;

  TracingConsumer(Consumer<K, V> consumer,
      Instrumenter<ReceivedRecords, Void> receiveInstrumenter,
      Instrumenter<ConsumerRecord<K, V>, Void> processInstrumenter) {
    this.consumer = consumer;
    this.receiveInstrumenter = receiveInstrumenter;
    this.processInstrumenter = processInstrumenter;
  }

  @Override
  public Set<TopicPartition> assignment() {
    return consumer.assignment();
  }

  @Override
  public Set<String> subscription() {
    return consumer.subscription();
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
    consumer.subscribe(topics, listener);
  }

  @Override
  public void subscribe(Collection<String> topics) {
    consumer.subscribe(topics);
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
    consumer.subscribe(pattern, listener);
  }

  public void subscribe(Pattern pattern) {
    consumer.subscribe(pattern);
  }

  @Override
  public void unsubscribe() {
    consumer.unsubscribe();
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    consumer.assign(partitions);
  }

  @Override
  @Deprecated
  public ConsumerRecords<K, V> poll(long timeout) {
    return poll(Duration.ofMillis(timeout));
  }

  public ConsumerRecords<K, V> poll(Duration duration) {
    return null; // TODO
  }

  @Override
  public void commitSync() {
    consumer.commitSync();
  }

  public void commitSync(Duration duration) {
    // consumer.commitSync(duration);
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    consumer.commitSync(offsets);
  }

  public void commitSync(Map<TopicPartition, OffsetAndMetadata> map, Duration duration) {
    // consumer.commitSync(map, duration);
  }

  @Override
  public void commitAsync() {
    consumer.commitAsync();
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    consumer.commitAsync(callback);
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
      OffsetCommitCallback callback) {
    consumer.commitAsync(offsets, callback);
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    consumer.seek(partition, offset);
  }

  public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
    // consumer.seek(partition, offsetAndMetadata);
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    consumer.seekToBeginning(partitions);
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    consumer.seekToEnd(partitions);
  }

  @Override
  public long position(TopicPartition partition) {
    return consumer.position(partition);
  }

  public long position(TopicPartition topicPartition, Duration duration) {
    // return consumer.position(topicPartition, duration);
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    return consumer.committed(partition);
  }

  public OffsetAndMetadata committed(TopicPartition topicPartition, Duration duration) {
    // return consumer.committed(topicPartition, duration);
  }

  public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
    // return consumer.committed(partitions);
  }

  public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions,
      final Duration timeout) {
    // return consumer.committed(partitions, timeout);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return consumer.metrics();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return consumer.partitionsFor(topic);
  }

  public List<PartitionInfo> partitionsFor(String s, Duration duration) {
    // return consumer.partitionsFor(s, duration);
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return consumer.listTopics();
  }

  public Map<String, List<PartitionInfo>> listTopics(Duration duration) {
    // return consumer.listTopics(duration);
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    consumer.pause(partitions);
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    consumer.resume(partitions);
  }

  @Override
  public Set<TopicPartition> paused() {
    return consumer.paused();
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch) {
    return consumer.offsetsForTimes(timestampsToSearch);
  }

  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> map,
      Duration duration) {
    // return consumer.offsetsForTimes(map, duration);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    return consumer.beginningOffsets(partitions);
  }

  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> collection,
      Duration duration) {
    // return consumer.beginningOffsets(collection, duration);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    return consumer.endOffsets(partitions);
  }

  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> collection,
      Duration duration) {
    // return consumer.endOffsets(collection, duration);
  }

  @Override
  public ConsumerGroupMetadata groupMetadata() {
    return consumer.groupMetadata();
  }

  @Override
  public void enforceRebalance() {
    consumer.enforceRebalance();
  }

  @Override
  public void close() {
    consumer.close();
  }

  @Override
  @Deprecated
  public void close(long l, TimeUnit timeUnit) {
    consumer.close(l, timeUnit);
  }

  @Override
  public void close(Duration duration) {
    consumer.close(duration);
  }

  @Override
  public void wakeup() {
    consumer.wakeup();
  }
}
