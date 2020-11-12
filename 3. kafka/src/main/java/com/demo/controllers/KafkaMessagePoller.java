package com.demo.controllers;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class KafkaMessagePoller {

	/**
	 * Consume message. This controller function polls the messages for a topic available in Kafka.
	 *
	 * @param topic the topic
	 * @return the string
	 */
	@RequestMapping(value = "/producer/consume-message/{topic}", method = { RequestMethod.GET })
	@ResponseBody
	public String consumeMessage(@PathVariable String topic) {
		
		ConsumerFactory<String, Object> consumerFactory = getConsumerFactoryInstance();

		Consumer<String, Object> consumer = consumerFactory.createConsumer();
		
		consumer.subscribe(Collections.singletonList("users"));
		
		// poll messages from last 10 days
		ConsumerRecords<String, Object> consumerRecords = consumer.poll(Duration.ofDays(10));

		// print on console or send back as a string/json. Feel free to change controller function implementation for ResponseBody
		consumerRecords.forEach(action -> {
			System.out.println(action.value());
		});
		
		return  "success";
	}
	
	public ConsumerFactory<String, Object> getConsumerFactoryInstance() {
		Map<String, Object> configs = new java.util.HashMap<>();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id2");
		configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
		configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
		ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
		return consumerFactory;
	}
}