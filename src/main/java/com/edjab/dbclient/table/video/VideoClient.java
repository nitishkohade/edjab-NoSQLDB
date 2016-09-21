//@Copyright Edjab 2016

package com.edjab.dbclient.table.video;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ItemCollectionSizeLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.edjab.dbclient.table.helper.TableHelper;
import com.edjab.model.Video;

/**
 * This is the Video Client Class whose act as the application class for all the retrieval, 
 * updates w.r.t NoSqlDB.
 * @author varun
 *
 */
public class VideoClient {

	private static final Logger LOGGER = Logger.getLogger(VideoClient.class);
	private static final String TABLE_NAME = "VideoEdjabProd";
	private static final String GLOBAL_SECONDARY_INDEX = "MostHelpfulVideosBySchoolEdjabProd";
	private final AmazonDynamoDBClient amazonDynamoDBClient;
	
	/**
	 * Default Constructor
	 * 1) Do some testing by adding client configuration and setting maxRetry method also see max connection method.
	 * It is not thread safe. this applies to all classes.
	 * @throws IOException
	 */
	public VideoClient() throws IOException {
		BasicConfigurator.configure();
		AWSCredentials credentials;
		try {
		       credentials= new PropertiesCredentials(
		    		   VideoClient.class
						.getResourceAsStream("/AwsCredentials.properties"));
		       if(credentials.getAWSAccessKeyId().isEmpty()) {
		    	   LOGGER.error("No credentials supplied in AwsCredentials.properties");
	            }
		}
		catch (IOException e) {
			LOGGER.error("Could not load credentials from file.", e);
            throw new RuntimeException(e);
        }
		amazonDynamoDBClient = new AmazonDynamoDBClient(credentials);
	}
	
	/**
	 * Constructor for Dependency Injection
	 * @param dynamoDBClient
	 * @throws IOException
	 */
	public VideoClient(final AmazonDynamoDBClient dynamoDBClient) 
			throws IOException {
		checkNotNull(dynamoDBClient, "dynamoDBClient must not be null");
		BasicConfigurator.configure();
		amazonDynamoDBClient = dynamoDBClient;
	}

	/**
	 * TODO: Tested. Change Partition
	 * @throws Exception
	 */
	public void createTable() throws Exception {
		try {
			
		LOGGER.info("Creating table " + TABLE_NAME);
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"country").withAttributeType("S"));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"videoid").withAttributeType("S"));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"schoolnameid").withAttributeType("S"));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"helpfulVotes").withAttributeType("N"));


		ArrayList<KeySchemaElement> tableKeySchemaElement = new ArrayList<KeySchemaElement>();
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("country").withKeyType(
				KeyType.HASH));
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("videoid").withKeyType(
				KeyType.RANGE));
		
		final ProvisionedThroughput tableProvisionedThroughput = new ProvisionedThroughput()
		.withReadCapacityUnits(1L).withWriteCapacityUnits(1L);
		
		final ProvisionedThroughput indexProvisionedThroughput = new ProvisionedThroughput()
		.withReadCapacityUnits(2L).withWriteCapacityUnits(1L);
		
		StreamSpecification streamSpecification = new StreamSpecification();
        streamSpecification.setStreamEnabled(true);
        streamSpecification.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);

		GlobalSecondaryIndex globalSecondaryIndex = new GlobalSecondaryIndex()
		.withIndexName(GLOBAL_SECONDARY_INDEX)
		.withProvisionedThroughput(indexProvisionedThroughput)
		.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
		
		ArrayList<KeySchemaElement> indexKeySchemaElement = new ArrayList<KeySchemaElement>();

		indexKeySchemaElement.add(new KeySchemaElement()
		    .withAttributeName("schoolnameid")
		    .withKeyType(KeyType.HASH));  //Partition key
		indexKeySchemaElement.add(new KeySchemaElement()
		    .withAttributeName("helpfulVotes")
		    .withKeyType(KeyType.RANGE));  //Sort key

		globalSecondaryIndex.setKeySchema(indexKeySchemaElement);
		

		final CreateTableRequest request = new CreateTableRequest()
				.withTableName(TABLE_NAME)
				.withAttributeDefinitions(attributeDefinitions)
				.withKeySchema(tableKeySchemaElement)
				.withProvisionedThroughput(tableProvisionedThroughput)
				.withGlobalSecondaryIndexes(globalSecondaryIndex)
				.withStreamSpecification(streamSpecification);

		final CreateTableResult result = amazonDynamoDBClient.createTable(request);
		LOGGER.info("Created table " + result.getTableDescription().getTableName());
		}
		catch(ResourceInUseException resourceInUseException) {
			LOGGER.info(TABLE_NAME + " has already been created");
		}
	}
	
	/**
	 * Tested. Uploads Video By User
	 * @param uploadedBy
	 * @param uploadedTo
	 * @param videoUrl
	 * @param videoDescriptiveName
	 * @return
	 * @throws InterruptedException
	 */
	public boolean uploadVideoByUser(final String uploadedBy, final String uploadedTo,
			final String videoUrl, final String videoDescriptiveName) throws InterruptedException {
		checkArgument(StringUtils.isNotBlank(uploadedBy), "UploadedBy field must not be blank");
		checkArgument(StringUtils.isNotBlank(uploadedTo), "UploadedTo field must not be blank");
		checkArgument(StringUtils.isNotBlank(videoUrl), "VideoUrl field must not be blank");
		checkArgument(StringUtils.isNotBlank(videoDescriptiveName), 
				"Video's DescriptiveName field must not be blank");
		try {
			final Video video = new Video(UUID.randomUUID().toString(), videoUrl, uploadedBy, uploadedTo, 
					new DateTime(DateTimeZone.UTC).toString(), 0, videoDescriptiveName);
		putItems(video);
		return true;
		}
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : uploadVideoByUser, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : uploadVideoByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : uploadVideoByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : uploadVideoByUser, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : uploadVideoByUser, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. putting all necessary items in the table
	 * @param video
	 */
	private void putItems(final Video video) {
		LOGGER.info("Putting items into table " + TABLE_NAME);
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		
		if(video != null) {
	     item.put("country", new AttributeValue().withS("INDIA"));	
	     item.put("videoid", new AttributeValue().withS(video.getVideoId()));	
		 item.put("videourl", new AttributeValue().withS(video.getVideoUrl()));
		 item.put("uploadedBy", new AttributeValue().withS(video.getUploadedBy()));
		 item.put("uploadedOn", new AttributeValue().withS(video.getUploadedOn().toString()));
		 item.put("schoolnameid", new AttributeValue().withS(video.getUploadedTo()));
		 item.put("helpfulVotes", new AttributeValue().withN(video.getHelpfulVotes().toString()));
		 item.put("descriptiveName", new AttributeValue().withS(video.getDescriptiveName()));
		
		final PutItemRequest itemRequest = new PutItemRequest().withTableName(
				TABLE_NAME).withItem(item);
		amazonDynamoDBClient.putItem(itemRequest);
		}
	}
	
	/**
	 * Tested. Gets Video
	 * @param videoid
	 * @return
	 */
	public Video getVideo(final String videoid) {
		checkArgument(StringUtils.isNotBlank(videoid), "VideoId field must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("country", new AttributeValue().withS("INDIA"));
		key.put("videoid", new AttributeValue().withS(videoid));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		
		String uploadedBy = "";
		String uploadedOn = "";
		String helpfulVotes = "";
		String descriptiveName = "";
		String videoUrl = "";
		String schoolnameid = "";
		
		for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
				String attributeName = item.getKey();
				String value = "";
				if(attributeName.equals("uploadedBy")) {
					value = item.getValue().getS();
					if(value != null) {
						uploadedBy =  value;
					}
				}
				if(attributeName.equals("schoolnameid")) {
					value = item.getValue().getS();
					if(value != null) {
						schoolnameid =  value;
					}
				}
				if(attributeName.equals("descriptiveName")) {
					value = item.getValue().getS();
					if(value != null) {
						descriptiveName =  value;
					}
				}
				if(attributeName.equals("uploadedOn")) {
					value = item.getValue().getS();
					if(value != null) {
						uploadedOn =  value;
					}
				}
				if(attributeName.equals("helpfulVotes")) {
					value = item.getValue().getN();
					if(value != null) {
						helpfulVotes =  value;
					}
				}
				if(attributeName.equals("videourl")) {
					value = item.getValue().getS();
					if(value != null) {
						videoUrl =  value;
					}
				}
		}
		return new Video(videoid, videoUrl, uploadedBy, schoolnameid, DateTime.parse(uploadedOn).toString(), 
				Integer.parseInt(helpfulVotes), descriptiveName);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : getVideo, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : getVideo, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : getVideo, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : getImage, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return null;
	}
	
	/**
	 * Tested. Deletes Video
	 * @param schoolnameid
	 * @param videoId
	 * @param uploadedBy
	 * @return
	 * @throws InterruptedException
	 */
		public boolean deleteVideo(final String schoolnameid, final String videoId, final String uploadedBy) 
				throws InterruptedException {
			checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid field must not be blank");
			checkArgument(StringUtils.isNotBlank(videoId), "VideoId field must not be blank");
			checkArgument(StringUtils.isNotBlank(uploadedBy), "UploadedBy field must not be blank");
			try {
			Map<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue>();
			HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("country", new AttributeValue().withS("INDIA"));
			key.put("videoid", new AttributeValue().withS(videoId));

			expectedValues.put("uploadedBy", new ExpectedAttributeValue()
					.withValue(new AttributeValue().withS(uploadedBy)));
			expectedValues.put("schoolnameid", new ExpectedAttributeValue()
					.withValue(new AttributeValue().withS(schoolnameid)));

			final ReturnValue returnValues = ReturnValue.ALL_OLD;

			final DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
					.withTableName(TABLE_NAME).withKey(key)
					.withExpected(expectedValues).withReturnValues(returnValues);

			final DeleteItemResult result = amazonDynamoDBClient.deleteItem(deleteItemRequest);

			// Check the response.
			LOGGER.info("Printing item that was deleted...");
			TableHelper.printItem(result.getAttributes());
			return true;
			} 
			catch(ConditionalCheckFailedException conditionalCheckFailedException) {
				LOGGER.error("API : deleteVideo, Error while condition checks", 
						conditionalCheckFailedException);
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : deleteVideo, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : deleteVideo, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
				LOGGER.error("API : deleteVideo, Item collection size limit  excceeded", 
						itemCollectionSizeLimitExceededException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : deleteVideo, Internal Server error", 
						internalServerErrorException);
			}
			return false;
		}
	
	/**
	 * Tested. Gets Videos by School
	 * @param schoolnameid
	 * @return
	 */
	public java.util.List<Video> getVideosBySchool(final String schoolnameid) {
		checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid field must not be blank");
		java.util.List<Video> videos = new ArrayList<Video>();	
		try {
			  final QuerySpec spec = new QuerySpec()
		      .withKeyConditionExpression("schoolnameid = :v_id")
		      .withValueMap(new ValueMap().withString(":v_id", schoolnameid))
		      .withScanIndexForward(false);
			
			  final DynamoDB dynamoDB = new DynamoDB(amazonDynamoDBClient);
		      final Table table = dynamoDB.getTable(TABLE_NAME);
			  final Index index = table.getIndex(GLOBAL_SECONDARY_INDEX);
		      final ItemCollection<QueryOutcome> items = index.query(spec);
		
		      final Iterator<Item> iterator = items.iterator();
		      Item item = null;
		      while (iterator.hasNext()) 
		      {
		        item = iterator.next();
		        Video video = new Video(
		        		item.get("videoid").toString(), 
		        		item.get("videourl").toString(), 
		        		item.get("uploadedBy").toString(), 
		        		item.get("schoolnameid").toString(), 
		        		item.get("uploadedOn").toString(), 
		        		Integer.parseInt(item.get("helpfulVotes").toString()),
		        		item.get("descriptiveName").toString());
		        videos.add(video);
		      }
		      return videos;
		}
		catch(Exception exception) {
			LOGGER.error("API : getVideosBySchool, Exception thrown while querying for all Videos", 
					exception);
		}
		
		return null;
	}
		
	public static void main(String[] args) {
		try {
			
			final VideoClient dbClient = new VideoClient();

			/*
			System.out.println(dbClient.uploadVideoByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
					"//www.youtube.com/embed/gUu-siay5Io", "Video1"));
			
			System.out.println(dbClient.uploadVideoByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
					"//www.youtube.com/embed/-kadTljuab4", "Video2"));
			
			System.out.println(dbClient.uploadVideoByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
					"//www.youtube.com/embed/CeHLn9qHXPs", "Video3"));
			*/
			//System.out.println(dbClient.uploadVideoByUser("varunkohade.iitr@gmail.com", 
			//		"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
			//		"//www.youtube.com/embed/JpvaOab6RJc", "Video4"));
			/*
			System.out.println(dbClient.uploadVideoByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA", 
					"//www.youtube.com/embed/gI0k_DqlyG8", "Video1"));
			
			System.out.println(dbClient.uploadVideoByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA", 
					"//www.youtube.com/embed/8VBZyibzae4", "Video2"));
			
			System.out.println(dbClient.uploadVideoByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA", 
					"//www.youtube.com/embed/3e5YJ7m6yW8", "Video3"));
			
			System.out.println(dbClient.uploadVideoByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA", 
					"//www.youtube.com/embed/bGm-Ti2P4qM", "Video4"));
			
			System.out.println(dbClient.uploadVideoByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA", 
					"//www.youtube.com/embed/-TVHSo2ke7k", "Video5"));
			*/
			
			//dbClient.createTable();
			//System.out.println(dbClient.getVideo("//www.youtube.com/embed/-kadTljuab4"));
			System.out.println(dbClient.getVideosBySchool("INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			//System.out.println(dbClient.deleteVideo("INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
			//		"553e3153-0d0d-45ee-80f9-86f2e805f589", "varunkohade.iitr@gmail.com"));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}