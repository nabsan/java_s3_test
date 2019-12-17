package com.amazonaws.samples;

import java.io.*;
import java.util.*;
	 
import com.amazonaws.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
	 
public class SampleSingleUpload {
	   
	  private static final int COUNTS = 100;
	  //例） 100 Uploaded : 4767 ms
	  
	  public static void main(String[] args) throws IOException {
	 
	    //AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(
	    //    PutS3Sample.class
	    //        .getResourceAsStream("AwsCredentials.properties")));
	    
		EndpointConfiguration endpoint = new EndpointConfiguration("http://192.168.100.10:4572", "192.168.100.10");

	    
        AmazonS3 s3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials (new AWSStaticCredentialsProvider(new BasicAWSCredentials("my-access-key-id", "my-secret-access-key")))
                .withEndpointConfiguration(endpoint).enablePathStyleAccess()
                .build();
	 
	    String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();
	    String key = "MyObjectKey";
	 
	    try {
	      System.out.println("Creating bucket " + bucketName + "\n");
	      s3.createBucket(bucketName);
	 
	      long startSingle = System.currentTimeMillis();
	 
	      for (int i = 0; i < COUNTS; i++) {
	        s3.putObject(new PutObjectRequest(bucketName, key
	            + UUID.randomUUID(), createSampleFile()));
	      }
	 
	      long endSingle = System.currentTimeMillis();
	      System.out.format("%d Uploaded : %d ms\n",COUNTS,
	          (endSingle - startSingle));
	 
	    } catch (AmazonClientException ace) {
	      System.out.println("Error Message: " + ace.getMessage());
	    }
	  }
	 
	  private static File createSampleFile() throws IOException {
	    File file = File.createTempFile("aws-java-sdk-", ".txt");
	    file.deleteOnExit();
	 
	    Writer writer = new OutputStreamWriter(new FileOutputStream(file));
	    writer.write("abcdefghijklmnopqrstuvwxyz\n");
	    writer.close();
	 
	    return file;
	  }
}


