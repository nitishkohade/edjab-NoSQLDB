//@Copyright Edjab 2016

package com.edjab.dbclient.table.image;

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
import com.edjab.model.Image;

/**
 *  This is the Image Client Class whose act as the application class for all the retrieval, 
 *  updates w.r.t NoSqlDB.
 * @author varun
 *
 */
public class ImageClient {


	private static final Logger LOGGER = Logger.getLogger(ImageClient.class);
	private static final String TABLE_NAME = "ImageEdjabProd";
	private static final String GLOBAL_SECONDARY_INDEX = "MostHelpfulImagesBySchoolEdjabProd";
	private final AmazonDynamoDBClient amazonDynamoDBClient;
	
	/**
	 * Default Constructor
	 * 1) Do some testing by adding client configuration and setting maxRetry method also see max connection method.
	 * It is not thread safe. this applies to all classes.
	 * @throws IOException
	 */
	public ImageClient() throws IOException {
		BasicConfigurator.configure();
		AWSCredentials credentials;
		try {
		       credentials= new PropertiesCredentials(
		    		   ImageClient.class
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
	public ImageClient(final AmazonDynamoDBClient dynamoDBClient) 
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
				"imageid").withAttributeType("S"));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"schoolnameid").withAttributeType("S"));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"helpfulVotes").withAttributeType("N"));


		ArrayList<KeySchemaElement> tableKeySchemaElement = new ArrayList<KeySchemaElement>();
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("country").withKeyType(
				KeyType.HASH));
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("imageid").withKeyType(
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
	 * Tested. Uploads Image By User
	 * @param uploadedBy
	 * @param uploadedTo
	 * @param imageUrl
	 * @param imageDescriptiveName
	 * @return
	 * @throws InterruptedException
	 */
	public boolean uploadImageByUser(final String uploadedBy, final String uploadedTo, 
			final String imageUrl, final String imageDescriptiveName) throws InterruptedException {
		checkArgument(StringUtils.isNotBlank(uploadedBy), "UploadedBy field must not be blank");
		checkArgument(StringUtils.isNotBlank(uploadedTo), "UploadedTo field must not be blank");
		checkArgument(StringUtils.isNotBlank(imageUrl), "ImageUrl field must not be blank");
		checkArgument(StringUtils.isNotBlank(imageDescriptiveName), 
				"Image's DescriptiveName field must not be blank");
		try {
			final Image image = new Image(
					UUID.randomUUID().toString(), 
					imageUrl, 
					uploadedBy, 
					uploadedTo, 
					new DateTime(DateTimeZone.UTC).toString(), 
					0, 
					imageDescriptiveName);
		putItems(image);
		return true;
		}
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : uploadImageByUser, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : uploadImageByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : uploadImageByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : uploadImageByUser, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : uploadImageByUser, Internal Server error", 
					internalServerErrorException);
		}catch (Exception exc) {
			exc.getMessage();
		}
		return false;
	}

	/**
	 * Tested. putting all necessary items in the table
	 * @param image
	 */
	private void putItems(final Image image) {
		LOGGER.info("Putting items into table " + TABLE_NAME);
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		
		if(image != null) {
		 item.put("country", new AttributeValue().withS("INDIA"));	
		 item.put("imageid", new AttributeValue().withS(image.getImageId()));
		 item.put("imageurl", new AttributeValue().withS(image.getImageUrl()));
		 item.put("uploadedBy", new AttributeValue().withS(image.getUploadedBy()));
		 item.put("uploadedOn", new AttributeValue().withS(image.getUploadedOn().toString()));
		 item.put("schoolnameid", new AttributeValue().withS(image.getUploadedTo()));
		 item.put("helpfulVotes", new AttributeValue().withN(image.getHelpfulVotes().toString()));
		 item.put("descriptiveName", new AttributeValue().withS(image.getDescriptiveName()));
		
		final PutItemRequest itemRequest = new PutItemRequest().withTableName(
				TABLE_NAME).withItem(item);
		amazonDynamoDBClient.putItem(itemRequest);
		}
	}
	
	/**
	 * Tested. Gets Image
	 * @param imageid
	 * @return
	 */
	public Image getImage(final String imageid) {
		checkArgument(StringUtils.isNotBlank(imageid), "ImageId field must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("country", new AttributeValue().withS("INDIA"));
		key.put("imageid", new AttributeValue().withS(imageid));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		
		String uploadedBy = "";
		String uploadedOn = "";
		String helpfulVotes = "";
		String descriptiveName = "";
		String imageUrl = "";
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
				if(attributeName.equals("imageurl")) {
					value = item.getValue().getS();
					if(value != null) {
						imageUrl =  value;
					}
				}
		}
		return new Image(
				imageid, 
				imageUrl, 
				uploadedBy, 
				schoolnameid, 
				DateTime.parse(uploadedOn).toString(), 
				Integer.parseInt(helpfulVotes), 
				descriptiveName);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : getImage, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : getImage, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : getImage, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : getImage, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return null;
	}
	
	/**
	 * Tested. Deletes image
	 * @param schoolnameid
	 * @param imageId
	 * @param uploadedBy
	 * @return
	 * @throws InterruptedException
	 */
		public boolean deleteImage(final String schoolnameid, final String imageId, 
				final String uploadedBy) throws InterruptedException {
			checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid field must not be blank");
			checkArgument(StringUtils.isNotBlank(imageId), "ImageId field must not be blank");
			checkArgument(StringUtils.isNotBlank(uploadedBy), "UploadedBy field must not be blank");
			try {
			Map<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue>();
			HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("country", new AttributeValue().withS("INDIA"));
			key.put("imageid", new AttributeValue().withS(imageId));

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
				LOGGER.error("API : deleteImage, Error while condition checks", 
						conditionalCheckFailedException);
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : deleteImage, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : deleteImage, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
				LOGGER.error("API : deleteImage, Item collection size limit  excceeded", 
						itemCollectionSizeLimitExceededException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : deleteImage, Internal Server error", 
						internalServerErrorException);
			}
			return false;
		}
	
		/**
		 * Tested. Gets images by school
		 * @param schoolnameid
		 * @return
		 */
	public java.util.List<Image> getImagesBySchool(final String schoolnameid) {
		checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid field must not be blank");
		java.util.List<Image> images = new ArrayList<Image>();	
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
		        Image image = new Image(
		        		item.get("imageid").toString(), 
		        		item.get("imageurl").toString(),
		        		item.get("uploadedBy").toString(),
		        		schoolnameid, 
		        		item.get("uploadedOn").toString(), 
		        		Integer.parseInt(item.get("helpfulVotes").toString()),
		        		item.get("descriptiveName").toString());
		        images.add(image);
		      }
		}
		catch(Exception exception) {
			LOGGER.error("API : getImagesBySchool, Exception thrown while querying for all Images", 
					exception);
		}
		
		return images;
	}
	
	public static void main(String[] args) {
		try {
			
			final ImageClient dbClient = new ImageClient();
			/*
			System.out.println(dbClient.uploadImageByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
					"https://upload.wikimedia.org/wikipedia/commons/3/30/Main%28Administrative%29Building_IIT-Roorkee.JPG", 
					"Image 1"));
			
			System.out.println(dbClient.uploadImageByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
					"https://upload.wikimedia.org/wikipedia/en/d/d4/Iitroorkee.jpg", 
					"Image 2"));
			
			
			System.out.println(dbClient.uploadImageByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
					"https://upload.wikimedia.org/wikipedia/commons/thumb/0/08/ThomasonCollegeOfEngineeringRoorkeeEst1847.jpg/800px-ThomasonCollegeOfEngineeringRoorkeeEst1847.jpg", 
					"Image 3"));
			*/
			System.out.println(dbClient.uploadImageByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
					"https://upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Admin_Block_IIT-R.JPG/800px-Admin_Block_IIT-R.JPG", 
					"Image 4"));
			/*
			System.out.println(dbClient.uploadImageByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
					"https://upload.wikimedia.org/wikipedia/commons/thumb/5/5b/ARD_IIT-Roorkee.jpg/800px-ARD_IIT-Roorkee.jpg", 
					"Image 5"));
			
			System.out.println(dbClient.uploadImageByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA", 
					"https://upload.wikimedia.org/wikipedia/en/thumb/5/58/IIT_Bombay_Logo.svg/300px-IIT_Bombay_Logo.svg.png", 
					"Image 1"));
			
			System.out.println(dbClient.uploadImageByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA", 
					"https://upload.wikimedia.org/wikipedia/commons/a/ae/IITB_Lawn.jpg", 
					"Image 2"));
			
			System.out.println(dbClient.uploadImageByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA", 
					"https://upload.wikimedia.org/wikipedia/commons/thumb/2/2a/Powai_Lake%2C_April_2012.JPG/800px-Powai_Lake%2C_April_2012.JPG", 
					"Image 3"));
			
			System.out.println(dbClient.uploadImageByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA", 
					"https://upload.wikimedia.org/wikipedia/commons/thumb/e/eb/IIT_Bombay_Olympic-size_Swimming_Pool.JPG/800px-IIT_Bombay_Olympic-size_Swimming_Pool.JPG", 
					"Image 4"));
			*/
			//dbClient.createTable();
			
			//System.out.println(dbClient.getImagesBySchool("INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			//System.out.println(dbClient.getImage("2f391060-660f-40fa-ac44-b0416f5aa0c8"));
		    
			//System.out.println(dbClient.deleteImage("INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
			//		"2f391060-660f-40fa-ac44-b0416f5aa0c8", "varunkohade.iitr@gmail.com"));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}