package com.gk.s3;

import java.io.BufferedReader;
/***
 * @author gauravkhandave
 */
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3BucketOperations {

	public static AmazonS3 getAWSS3Client() {

		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("GauravKhandave").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (/Users/gauravkhandave/.aws/credentials), and is in valid format.", e);
		}

		AmazonS3 s3 = new AmazonS3Client(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		s3.setRegion(usWest2);

		return s3;
	}

	public void createBucket(AmazonS3 s3, String bucketName) {

		System.out.println("Creating bucket " + bucketName + "\n");
		s3.createBucket(bucketName);

		System.out.println("Listing buckets");
		for (Bucket bucket : s3.listBuckets()) {
			System.out.println(" - " + bucket.getName());
		}
		System.out.println();
	}

	public void uploadObjectToBucket(AmazonS3 s3, String bucketName, String key, File file) {
		System.out.println("Uploading a new object to S3 from a file\n");
		s3.putObject(new PutObjectRequest(bucketName, key, file));
	}

	public void downloadObject(AmazonS3 s3, String bucketName, String key) {
		System.out.println("Downloading an object");
		S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
		System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());

		BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
		while (true) {
			try {
				String line = reader.readLine();
				if (line == null)
					break;

				System.out.println("    " + line);
			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static void main(String[] args) {

		String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();
		String key = "MyObjectKey";

		AmazonS3 s3 = getAWSS3Client();

		S3BucketOperations s3Operations = new S3BucketOperations();

		s3Operations.createBucket(s3, bucketName);

		s3Operations.uploadObjectToBucket(s3, bucketName, key, new File("upload/Hello.txt"));
		
		s3Operations.downloadObject(s3, bucketName, key);

	}

}
