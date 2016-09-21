//@Copyright Edjab 2016

package com.edjab.dbclient.table.userprofile;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ItemCollectionSizeLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.edjab.dbclient.table.helper.TableHelper;
import com.edjab.dbclient.table.image.ImageClient;
import com.edjab.model.ImageUrl;

/**
 * This is the UserToImage Client Class whose act as the application class for all the retrieval, 
 * updates w.r.t NoSqlDB
 * @author varun
 *
 */
public class UserToImageClient {

	private static final Logger LOGGER = Logger.getLogger(UserToImageClient.class);
	private static final String TABLE_NAME = "UserToImageEdjabProd";
	private final AmazonDynamoDBClient amazonDynamoDBClient;
	private final UserProfileClient userProfileClient;
	private final ImageClient imageClient;
	
	/**
	 * Default Constructor
	 * 1) Do some testing by adding client configuration and setting maxRetry method also see max connection method.
	 * It is not thread safe. this applies to all classes.
	 * @throws IOException
	 */
	public UserToImageClient() throws IOException {
		BasicConfigurator.configure();
		AWSCredentials credentials;
		try {
		       credentials= new PropertiesCredentials(
		    		   UserToImageClient.class
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
		userProfileClient = new UserProfileClient(amazonDynamoDBClient);
		imageClient = new ImageClient(amazonDynamoDBClient);
	}
	
	/**
	 * Constructor for Dependency Injection
	 * @param dynamoDBClient
	 * @throws IOException
	 */
	public UserToImageClient(final AmazonDynamoDBClient dynamoDBClient) throws IOException {
		checkNotNull(dynamoDBClient, "dynamoDBClient must not be null");
		BasicConfigurator.configure();
		amazonDynamoDBClient = dynamoDBClient;
		userProfileClient = new UserProfileClient(dynamoDBClient);
		imageClient = new ImageClient(dynamoDBClient);
	}
	
	/**
	 * TODO: Tested Change partition. Here making only one partition due to throughput constraint. 
	 * So country with value "INDIA" is chosen as partition key
	 */
	public void createTable() {
		try {
			
		LOGGER.info("Creating table " + TABLE_NAME);
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"country").withAttributeType("S"));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"imageid_userid").withAttributeType("S"));


		ArrayList<KeySchemaElement> tableKeySchemaElement = new ArrayList<KeySchemaElement>();
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("country").withKeyType(
				KeyType.HASH));
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("imageid_userid").withKeyType(
				KeyType.RANGE));
		
		final ProvisionedThroughput tableProvisionedThroughput = new ProvisionedThroughput()
		.withReadCapacityUnits(1L).withWriteCapacityUnits(1L);
		
		StreamSpecification streamSpecification = new StreamSpecification();
        streamSpecification.setStreamEnabled(true);
        streamSpecification.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);

		final CreateTableRequest request = new CreateTableRequest()
				.withTableName(TABLE_NAME)
				.withAttributeDefinitions(attributeDefinitions)
				.withKeySchema(tableKeySchemaElement)
				.withProvisionedThroughput(tableProvisionedThroughput)
				.withStreamSpecification(streamSpecification);

		final CreateTableResult result = amazonDynamoDBClient.createTable(request);
		LOGGER.info("Created table " + result.getTableDescription().getTableName());
		}
		catch(ResourceInUseException resourceInUseException) {
			LOGGER.info(TABLE_NAME + " has already been created");
		}
	}
	
	    /**
	     * Tested. Puts Image By User items
	     * @param userid
	     * @param imageid
	     * @param imageurl
	     * @return
	     */
		public boolean putImageByUser(final String userid, final String  imageid, final String imageurl) {
			checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
			checkArgument(StringUtils.isNotBlank(imageid), "Imageid must not be blank");
			checkArgument(StringUtils.isNotBlank(imageurl), "Imageurl must not be blank");
			if(userProfileClient.IsValidUserProfile(userid) == false) {
				return false;
			}
			if(imageClient.getImage(imageid) == null) {
				return false;
			}
			if(isImageAccessedByUser(userid, imageid) == true) {
				return false;
			}
			try {
				  LOGGER.info("Putting items into table " + TABLE_NAME);
				  Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
				  item.put("country", new AttributeValue().withS("INDIA"));
				  item.put("imageid_userid", new AttributeValue().withS(imageid + "_" + userid));
				  item.put("imageid", new AttributeValue().withS(imageid));
				  item.put("imageurl", new AttributeValue().withS(imageurl));
				  
				  final PutItemRequest itemRequest = new PutItemRequest().withTableName(
						TABLE_NAME).withItem(item);
				  amazonDynamoDBClient.putItem(itemRequest);   			
			      return true;
			}
			catch(ConditionalCheckFailedException conditionalCheckFailedException) {
				LOGGER.error("API : putImageByUser, Error while condition checks", 
						conditionalCheckFailedException);
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : putImageByUser, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : putImageByUser, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
				LOGGER.error("API : putImageByUser, Item collection size limit  excceeded", 
						itemCollectionSizeLimitExceededException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : putImageByUser, Internal Server error", 
						internalServerErrorException);
			}
			return false;
		}
	
    /**
     * Tested. Is Images Accessed By User i.e it ishelpful for user
     * @param userid
     * @param imageid
     * @return
     */
	public boolean isImageAccessedByUser(final String userid, final String imageid) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
		checkArgument(StringUtils.isNotBlank(imageid), "Imageid must not be blank");
		
		try{
		  Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		  key.put("country", new AttributeValue().withS("INDIA"));
		  key.put("imageid_userid", new AttributeValue().withS(imageid + "_" + userid));
	      final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		  final Map<String, AttributeValue> items = getItemResult.getItem();
		  if(items != null) {
			  return true;
		  }
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : IsImageAccessedByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : IsImageAccessedByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : IsImageAccessedByUser, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	    /**
	     * Tested. Deletes table's row
	     * @param userid
	     * @param imageid
	     * @return
	     */
		public boolean deleteUserToImageItem(final String userid, final String imageid) {
			checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
			checkArgument(StringUtils.isNotBlank(imageid), "Imageid must not be blank");
			try {
			   HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			   key.put("country", new AttributeValue().withS("INDIA"));
			   key.put("imageid_userid", new AttributeValue().withS(imageid + "_" + userid));

			   final ReturnValue returnValues = ReturnValue.ALL_OLD;
			   final DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
					.withTableName(TABLE_NAME).withKey(key).withReturnValues(returnValues);

			   final DeleteItemResult result = amazonDynamoDBClient.deleteItem(deleteItemRequest);

			   // Check the response.
			   LOGGER.info("Printing item that was deleted...");
			   TableHelper.printItem(result.getAttributes());
			   return true;
			} 
			catch(ConditionalCheckFailedException conditionalCheckFailedException) {
				LOGGER.error("API : deleteUserToImageItem, Error while condition checks", 
						conditionalCheckFailedException);
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : deleteUserToImageItem, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : deleteUserToImageItem, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
				LOGGER.error("API : deleteUserToImageItem, Item collection size limit  excceeded", 
						itemCollectionSizeLimitExceededException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : deleteUserToImageItem, Internal Server error", 
						internalServerErrorException);
			}
			catch(NullPointerException nullPointerException) {
				LOGGER.error("API : deleteUserToImageItem, NullPointerException possibly because of bad parameter", 
					nullPointerException);
			}
			return false;
		}
		
		/**
		 * Gets Images Accessed By user
		 * @param userid
		 * @return
		 */
		public java.util.List<ImageUrl> getImagesAccessedByUser(final String userid) {
			checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
			java.util.List<ImageUrl> images = new ArrayList<ImageUrl>();	
			try {
				  final QuerySpec spec = new QuerySpec()
					.withKeyConditionExpression("country = :v_id and contains(imageid_userid, :r_id)")
		            .withValueMap(new ValueMap()
		                   .withString(":v_id", "INDIA")
		                   .withString(":r_id", userid));
				
				  final DynamoDB dynamoDB = new DynamoDB(amazonDynamoDBClient);
			      final Table table = dynamoDB.getTable(TABLE_NAME);
			      final ItemCollection<QueryOutcome> items = table.query(spec);
			
			      final Iterator<Item> iterator = items.iterator();
			      Item item = null;
			      while (iterator.hasNext()) 
			      {
			        item = iterator.next();
			        ImageUrl imageUrl = new ImageUrl(item.get("imageurl").toString());
			        images.add(imageUrl);
			      }
			      return images;
			}
			catch(Exception exception) {
				LOGGER.error("API : getImagesAccessedByUser, Exception thrown while querying for all Images accessed"
						+ "by user", 
						exception);
			}
			
			return null;
		}
	
	public static void main(String[] args) {
		try {
			
			final UserToImageClient dbClient = new UserToImageClient();
			
			//dbClient.createTable();
			System.out.println(dbClient.putImageByUser("varunkohade.iitr@gmail.com", 
					"6bd5a74a-248f-4fdb-be52-c606347b2d56", 
					"https://upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Admin_Block_IIT-R.JPG/800px-Admin_Block_IIT-R.JPG"));
			//System.out.println(dbClient.deleteUserToImageItem("varunkohade.iitr@gmail.com", 
			//		"5e8b4ef1-caea-4220-839a-006ca549d1d9"));
			//System.out.println(dbClient.isImageAccessedByUser("varunkohade.iitr@gmail.com", 
			//				"5e8b4ef1-caea-4220-839a-006ca549d1d9"));
			//System.out.println(dbClient.getImagesAccessedByUser("varunkohade.iitr@gmail.com"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}