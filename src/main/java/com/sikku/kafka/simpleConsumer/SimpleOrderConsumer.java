package com.sikku.kafka.simpleConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class SimpleOrderConsumer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, Integer> consumer = new KafkaConsumer<String, Integer>(props);
		List<PartitionInfo> partitionInfos = consumer.partitionsFor("SimpleConsumerTopic");

		ArrayList<TopicPartition> partitions = new ArrayList<>();
		
//		for (PartitionInfo info : partitionInfos) {
//			partitions.add(new TopicPartition("SimpleConsumerTopic", info.partition()));
//		}

		partitionInfos.forEach(info -> partitions.add(new TopicPartition("SimpleConsumerTopic", info.partition())));

		consumer.assign(partitions);

		ConsumerRecords<String, Integer> orders = consumer.poll(Duration.ofSeconds(20));
		for (ConsumerRecord<String, Integer> order : orders) {
			System.out.println(order.key() + " " + order.value());
		}
		consumer.close();

	}

}
