package com.sikku.kafka.orderConsumer.customserializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class OrderDeSerializer implements Deserializer<Order> {


	@Override
	public Order deserialize(String topic, byte[] data) {
		Order order = null;
		ObjectMapper mapper = new ObjectMapper();
		try {
			order = mapper.readValue(data, Order.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return order;
	}

}
