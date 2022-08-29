package com.example.demo;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

public class AwsKinesisClient {

    public static AmazonKinesis getKinesisClient(){
        return AmazonKinesisClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }
}
