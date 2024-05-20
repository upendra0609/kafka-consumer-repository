package com.sikku.kafka.orderConsumer.customserializers;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

public class OrderConsumer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "com.sikku.kafka.orderProducer01.customserializers.OrderDeSerializer");
		props.setProperty("group.id", "OrderCSGroup");
		props.setProperty("auto.commit.offset", "false");

		Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

		KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);

		// class RebalanceHandler to handle onPartitionsRevoked and onPartitionsAssigned

		class RebalanceHandler implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				consumer.commitSync(currentOffsets);
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				// TODO Auto-generated method stub

			}

		}

		consumer.subscribe(Collections.singletonList("OrderPartitionedTopic"), new RebalanceHandler());
		try {
			while (true) {
				ConsumerRecords<String, Order> orders = consumer.poll(Duration.ofSeconds(20));
				int count = 0;
				for (ConsumerRecord<String, Order> order : orders) {
					System.out.println(order.key() + " " + order.value());
					
					currentOffsets.put(new TopicPartition(order.topic(), order.partition()),
							new OffsetAndMetadata(order.offset() + 1));
					
					if (count % 10 == 10) {
						consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {

							@Override
							public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
									Exception exception) {
								if (exception != null) {
									System.out.println("Commit failed for offset " + offsets);
								}

							}
						});
					}
					count++;
				}
//				consumer.commitSync();
//				consumer.commitAsync();
			}
		} finally {
			consumer.close();
		}

	}

}
