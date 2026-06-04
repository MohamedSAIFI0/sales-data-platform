package io.confluent.connect.s3.storage;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

public class MinioS3Storage extends S3Storage {

    public MinioS3Storage(S3SinkConnectorConfig conf, String url) {
        super(conf, url);
    }

    @Override
    public boolean bucketExists() {
        return true;
    }

    @Override
    public AmazonS3 newS3Client(S3SinkConnectorConfig conf) {
        String endpoint = conf.getString("s3.endpoint");
        String accessKey = conf.getPassword("aws.access.key.id").value();
        String secretKey = conf.getPassword("aws.secret.access.key").value();
        String region = conf.getString("s3.region");

        BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);

        return AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
            .withCredentials(new AWSStaticCredentialsProvider(creds))
            .withPathStyleAccessEnabled(true)
            .build();
    }
}