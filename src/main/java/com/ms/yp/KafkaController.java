package com.ms.yp;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {
	Properties config = null;
	AdminClient admin = null;
	String dlq = null;

	@Autowired KafkaTemplate<String, String> kafkaTemplate;

	public KafkaController() {
		this.dlq = "dead-letter-topic";
		this.config = new Properties();
		// config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.11.13.188:9092");
		admin = AdminClient.create(config);
	}

	@RequestMapping("/hello")
    @ResponseBody
    public String helloWorld() {
        return "Hello World!";
    }

	@GetMapping("/kafka/admin/createTopic/{topic}")
	public String createTopic(@PathVariable("topic") final String topic)
			throws InterruptedException, ExecutionException
	{
		NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
		CreateTopicsResult createResult = admin.createTopics(Collections.singleton(newTopic));

		//listing
		System.out.println("-- listing --");
		admin.listTopics().names().get().forEach(System.out::println);
		KafkaFuture<Void> future = createResult.values().get(topic);
		if (future.isCompletedExceptionally()) {
			return "Failed to create topic: " + future.get();
		}
		String response = "New topics list is " + admin.listTopics().names().get() +
				System.lineSeparator() + "Created topic with configs " + future.get();
		System.out.println(response);
		return response;
	}

	@GetMapping("/kafka/admin/deleteTopic/{topic}")
	public String deleteTopic(@PathVariable("topic") final String topic)
			throws InterruptedException, ExecutionException
	{
		List<String> deleteTopics = List.of(topic);
		DeleteTopicsResult deleteTopicResult = admin.deleteTopics(deleteTopics);

		admin.listTopics().names().get().forEach(System.out::println);
		KafkaFuture<Void> future = deleteTopicResult.topicNameValues().get(topic);
		if (future.isCompletedExceptionally()) {
			return "Failed to create topic: " + future.get();
		}
		String response = "New topics list is " + admin.listTopics().names().get() +
				System.lineSeparator() + "Deleted topic with configs " + future.get();
		System.out.println(response);
		return response;
	}

	@GetMapping("/kafka/admin/listTopics")
	public String listTopics() throws InterruptedException, ExecutionException
	{
		KafkaFuture<Set<String>> future = admin.listTopics().names();
		if (future.isCompletedExceptionally()) {
			return "Failed to list topics" + future.get();
		}
		/* for(String s: admin.listTopics().names().get())
			System.out.println(s); */
		String response = admin.listTopics().names().get().toString();
		System.out.println(response);
		return response;
	}

	@GetMapping("/kafka/admin/describeTopic/{topic}")
	public String describeTopic(@PathVariable("topic") final String topic)
			throws InterruptedException, ExecutionException
	{
		List<String> topics = List.of(topic);
		TopicCollection.ofTopicNames(topics);

		DescribeTopicsResult describeTopicsResult = admin.describeTopics(topics);

		KafkaFuture<TopicDescription> future = describeTopicsResult.topicNameValues().get(topic);
		if (future.isCompletedExceptionally()) {
			return "Failed to create topic: " + future.get();
		}
		String response = future.get().toString();
		System.out.println(response);
		return response;
	}

	@PostMapping(value = "/kafka/produce/{topic}"
			// , consumes = {MediaType.APPLICATION_JSON_VALUE}
			, consumes = {"text/plain", "application/*"}
	)  // https://stackoverflow.com/a/29330280
	public String produceMessage(@PathVariable("topic") final String topic,
			@RequestBody KafkaRecord rec) { // throws Exception {
		try {
			// if (rec.getKey() == null) {
			//	throw new Exception("Message can't be null/empty");
			// }
			CompletableFuture<SendResult<String, String>> sentResult = kafkaTemplate
					.send(topic, rec.getPartition(), rec.getTimestamp(), rec.getKey(), rec.getMessage());
			return "Published Successfully to topic " + sentResult.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// e.printStackTrace();
			if (rec.getKey() == null) {
				rec.setKey("key1");
			}
			produceMessage(this.dlq, rec);
		}
		return null;
	}
}
