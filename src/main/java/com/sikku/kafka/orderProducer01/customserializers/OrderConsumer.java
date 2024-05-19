package com.sikku.kafka.orderProducer01.customserializers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class OrderConsumer {

	public static void main(String[] args) {

		
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "com.sikku.kafka.orderProducer01.customserializers.OrderDeSerializer");
		props.setProperty("group.id", "OrderCSGroup");

		KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList("OrderPartitionedTopic"));
		try {
			while (true) {
				ConsumerRecords<String, Order> orders = consumer.poll(Duration.ofSeconds(20));
				for (ConsumerRecord<String, Order> order : orders) {
					System.out.println(order.key() + " " + order.value());
					System.out.println("Partition: "+order.partition());
				}
			}
		} finally {
			consumer.close();
		}
					
	}

}
