package com.amazonaws.samples;

import java.io.*;
import java.util.*;
	 
import com.amazonaws.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
//import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
	 

	   
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
	 
	class S3UploadTask2 implements Callable<Boolean> {
	  private String bucket = null;
	  private String key = null;
	 
	  public S3UploadTask2(String bucket, String key) {
	    this.bucket = bucket;
	    this.key = key;
	  }
	 
	  @Override
	  public Boolean call() {
	    try {
	       //AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(
	       //   S3UploadTask.class
	       //       .getResourceAsStream("AwsCredentials.properties")));
	      
			EndpointConfiguration endpoint = new EndpointConfiguration("http://192.168.100.10:4572", "192.168.100.10");

		    
	        AmazonS3 s3 = AmazonS3ClientBuilder
	                .standard()
	                .withCredentials (new AWSStaticCredentialsProvider(new BasicAWSCredentials("my-access-key-id", "my-secret-access-key")))
	                .withEndpointConfiguration(endpoint).enablePathStyleAccess()
	                .build();
	        
            S3Object object = s3.getObject(new GetObjectRequest(bucket, key));
            System.out.println("[check]Content-Type: "  + object.getObjectMetadata().getContentType() +","+ key);
            
            if (object.getObjectMetadata().getContentType().equals("application/octet-stream")) {
	            ObjectMetadata metadata = new ObjectMetadata();
	            //metadata.setContentType("application/octet-stream");
	            metadata.setContentType("text/html");
	            final CopyObjectRequest request = new CopyObjectRequest(bucket, key, bucket, key)
	                    .withSourceBucketName( bucket )
	                    .withSourceKey(key)
	                    .withNewObjectMetadata(metadata);
	            s3.copyObject(request);
            }
	 
	      return true;
	    } catch (Exception e) {
	      return false;
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
	 
	
	//Total time - 7768 ms
	//Total time - 17601 ms

	public class SampleMultiMimeUpload {
	 
	  private static final int CLIENTS = 100;
	  private static final int THREADS = 10;
	 
	  private static final ExecutorService executorPool = Executors
	      .newFixedThreadPool(THREADS);
	 
	  public static void main(String[] args) throws IOException {
	    int success = 0;
	    int failure = 0;
	 
	    //AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(
	    //    ConcurrentUpload.class
	    //        .getResourceAsStream("AwsCredentials.properties")));
	    
		EndpointConfiguration endpoint = new EndpointConfiguration("http://192.168.100.10:4572", "192.168.100.10");

	    
        AmazonS3 s3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials (new AWSStaticCredentialsProvider(new BasicAWSCredentials("my-access-key-id", "my-secret-access-key")))
                .withEndpointConfiguration(endpoint).enablePathStyleAccess()
                .build();
	 
	    String bucket = "my-first-s3-bucket-73b9af6b-f282-499c-840f-5b47557acf5f";
	    System.out.println("bktname="+bucket);
	    //String key = "MyObjectKey" + UUID.randomUUID();
	 
	    
	    Collection<S3UploadTask2> collection = new ArrayList<S3UploadTask2>();
	    
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
      	      .withBucketName(bucket)
                //.withPrefix("myfolderinthebucket") //child folder if exists
                .withEncodingType("url");
        ObjectListing objectListing = s3.listObjects(listObjectsRequest);
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          //System.out.println(" - " + objectSummary.getKey() + "  " +
          //                   "(size = " + objectSummary.getSize() + ")");
    	    //for (int i = 0; i < CLIENTS; i++) {
    		      S3UploadTask2 task = new S3UploadTask2(bucket, objectSummary.getKey());
    		      collection.add(task);
    		//}
        }
        System.out.println();
	   

	 
	    long startTime = System.currentTimeMillis();
	    try {
	        List<Future<Boolean>> list = executorPool.invokeAll(collection);
	        for (Future<Boolean> fut : list) {
	          int ignore = fut.get() ? success++ : failure++;
	        }
	    	
	    	
//            ObjectListing list2 = s3.listObjects(bucket);
//        	do {
//        		for (S3ObjectSummary s : list2.getObjectSummaries()) {
//        			//System.out.printf("bucket=%s, key=%s, size=%d\n", s.getBucketName(), s.getKey(), s.getSize());
//                    S3Object object = s3.getObject(new GetObjectRequest(bucket, s.getKey()));
//                    System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType() +","+ s.getKey() + "," + s.getSize());
//        		}
//        		
//        		list2 = s3.listNextBatchOfObjects(list2);
//        	} while (list2.getMarker() != null);
	    } catch (Exception e) {
	      e.printStackTrace();
	    } finally {
	      System.out.println("TOTAL SUCCESS - " + success);
	      System.out.println("TOTAL FAILURE - " + failure);
	      System.out.println("Total time - "
	          + (System.currentTimeMillis() - startTime) + " ms");
	      executorPool.shutdown();
	    }
	  }
	}
