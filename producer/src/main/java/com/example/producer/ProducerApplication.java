package com.example.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

@SpringBootApplication
public class ProducerApplication {


	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(ProducerApplication.class, args);
		AmazonKinesis kinesisClient = AwsKinesisClient.getKinesisClient();
		while(true){
			sendData(kinesisClient);
			Thread.sleep(10000);
		}
	}

	private static void sendData(AmazonKinesis kinesisClient)  {
		//2. PutRecordRequest
		PutRecordsRequest recordsRequest = new PutRecordsRequest();
		recordsRequest.setStreamName("raw-data-stream");
		recordsRequest.setRecords(getRecordsRequestList());

		PutRecordsResult results = kinesisClient.putRecords(recordsRequest);
		if(results.getFailedRecordCount() > 0){
			System.out.println("Error occurred for records " + results.getFailedRecordCount());
		} else {
			System.out.println(results);
			System.out.println("Data sent successfully...");
		}

	}

	private static List<PutRecordsRequestEntry> getRecordsRequestList(){
		Scanner sc = null;
		try {
			sc = new Scanner(new File("E:\\sample.txt"));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		sc.useDelimiter("\n");
		List<PutRecordsRequestEntry> putRecordsRequestEntries = new ArrayList<>();

		while(sc.hasNext()){
			PutRecordsRequestEntry requestEntry = new PutRecordsRequestEntry();
			requestEntry.setData(ByteBuffer.wrap(sc.next().getBytes()));
			requestEntry.setPartitionKey(UUID.randomUUID().toString());
			putRecordsRequestEntries.add(requestEntry);
		}
		return putRecordsRequestEntries;
	}

}
