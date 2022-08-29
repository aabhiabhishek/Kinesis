package com.example.demo;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesis.model.Record;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@SpringBootApplication
public class DemoApplication {



	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(DemoApplication.class, args);

		AmazonKinesis kinesisClient = AwsKinesisClient.getKinesisClient();

		while(true) {
			getData(kinesisClient);
			Thread.sleep(10000);
		}
	}


	public static void getData(AmazonKinesis kinesisClient){

		GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();

		getShardIteratorRequest.setStreamName("raw-data-stream");
		getShardIteratorRequest.setShardId("shardId-000000000000");
		getShardIteratorRequest.setShardIteratorType(ShardIteratorType.LATEST.name());

		GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);

		// Get the shard iterator from the Shard Iterator Response
		String shardIterator = getShardIteratorResult.getShardIterator();

		while (shardIterator != null) {
			List<String> data = new ArrayList<>();
			// Prepare the get records request with the shardIterator
			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
			getRecordsRequest.setShardIterator(shardIterator);

			// Read the records from the shard
			GetRecordsResult getRecordsRes = kinesisClient.getRecords(getRecordsRequest);

			List<Record> records = getRecordsRes.getRecords();

			if(!CollectionUtils.isEmpty(records)) {
				System.out.println("Original data");
				records.forEach(record -> {
					byte[] dataInBytes = record.getData().array();
					String s = new String(dataInBytes);
					data.add(s);
					System.out.println(s);
				});


				//process data and produce it to new stream
				processData(data);
			}

			shardIterator = getRecordsRes.getNextShardIterator();
		}

	}

	public static void processData(List<String> data){
		Random random = new Random();
		List<PutRecordsRequestEntry> putRecordsRequestEntries = new ArrayList<>();
		List<String> processedData = new ArrayList<>();
		data.stream().forEach(s -> {
			String s1 = random.nextInt() + ", ";
			processedData.add(s1.concat(s));
		});

		System.out.println("Processed data");
		processedData.forEach(p -> System.out.println(p));

		produceData(processedData);
	}

	public static void produceData(List<String> processedData){
		List<PutRecordsRequestEntry> putRecordsRequestEntries = new ArrayList<>();

		PutRecordsRequestEntry requestEntry = new PutRecordsRequestEntry();

		processedData.forEach(p -> {
			requestEntry.setData(ByteBuffer.wrap(p.getBytes()));
			requestEntry.setPartitionKey(UUID.randomUUID().toString());
			putRecordsRequestEntries.add(requestEntry);
		});

		PutRecordsRequest recordsRequest = new PutRecordsRequest();
		recordsRequest.setStreamName("processed-data-stream");
		recordsRequest.setRecords(putRecordsRequestEntries);

		AmazonKinesis amazonKinesis = AmazonKinesisClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		PutRecordsResult results = amazonKinesis.putRecords(recordsRequest);
		if(results.getFailedRecordCount() > 0){
			System.out.println("Error occurred for records " + results.getFailedRecordCount());
		} else {
			System.out.println(results);
			System.out.println("Data sent successfully...");
		}

	}

}
