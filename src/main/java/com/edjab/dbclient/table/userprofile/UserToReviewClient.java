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
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.edjab.dbclient.table.helper.TableHelper;
import com.edjab.dbclient.table.review.ReviewClient;
import com.edjab.model.ReviewId;

/**
 * This is the UserToReview Client Class whose act as the application class for all the retrieval,
 * updates w.r.t NoSqlDB
 * @author varun
 *
 */
public class UserToReviewClient {

	private static final Logger LOGGER = Logger.getLogger(UserToReviewClient.class);
	private static final String TABLE_NAME = "UserToReviewEdjabProd";
	private final AmazonDynamoDBClient amazonDynamoDBClient;
	private final UserProfileClient userProfileClient;
	private final ReviewClient reviewClient;
	
	/*
	 * Default constructor
	 */
	public UserToReviewClient() throws IOException {
		BasicConfigurator.configure();
		AWSCredentials credentials;
		try {
		       credentials= new PropertiesCredentials(
		    		   UserToReviewClient.class
						.getResourceAsStream("/AwsCredentials.properties"));
		       if(credentials.getAWSAccessKeyId().isEmpty()) {
	                System.err.println("No credentials supplied in AwsCredentials.properties");
	            }
		}
		catch (IOException e) {
            System.err.println("Could not load credentials from file.");
            throw new RuntimeException(e);
        }
		amazonDynamoDBClient = new AmazonDynamoDBClient(credentials);
		userProfileClient = new UserProfileClient(amazonDynamoDBClient);
		reviewClient = new ReviewClient(amazonDynamoDBClient);
	}
	
	/*
	 * Constructor for Dependency Injection
	 */
	public UserToReviewClient(final AmazonDynamoDBClient dynamoDBClient) throws IOException {
		checkNotNull(dynamoDBClient, "dynamoDBClient must not be null");
		BasicConfigurator.configure();
		amazonDynamoDBClient = dynamoDBClient;
		userProfileClient = new UserProfileClient(dynamoDBClient);
		reviewClient = new ReviewClient(dynamoDBClient);
	}
	
	//TODO: Tested. Change Partition
	//Tested
	public void createTable() {
		try {
			
		LOGGER.info("Creating table " + TABLE_NAME);
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"country").withAttributeType("S"));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"reviewid_userid").withAttributeType("S"));


		ArrayList<KeySchemaElement> tableKeySchemaElement = new ArrayList<KeySchemaElement>();
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("country").withKeyType(
				KeyType.HASH));
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("reviewid_userid").withKeyType(
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
	 * Tested. Puts Review By User items
	 * @param userid
	 * @param reviewid
	 * @return
	 */
		public boolean putReviewByUser(final String userid, final String reviewid) {
			checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
			checkArgument(StringUtils.isNotBlank(reviewid), "Reviewid Alias must not be blank");
			if(userProfileClient.IsValidUserProfile(userid) == false) {
				return false;
			}
			if(reviewClient.isReview(reviewid) == false) {
				return false;
			}
			if(isReviewAccessedByUser(userid, reviewid) == true) {
				return false;
			}
			try {
				  LOGGER.info("Putting items into table " + TABLE_NAME);
				  Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
				  item.put("country", new AttributeValue().withS("INDIA"));
				  item.put("reviewid_userid", new AttributeValue().withS(reviewid + "_" + userid));
				  item.put("reviewid", new AttributeValue().withS(reviewid));
				  final PutItemRequest itemRequest = new PutItemRequest().withTableName(
						TABLE_NAME).withItem(item);
				  amazonDynamoDBClient.putItem(itemRequest);   			
			      return true;
			}
			catch(ConditionalCheckFailedException conditionalCheckFailedException) {
				LOGGER.error("API : putReviewByUser, Error while condition checks", 
						conditionalCheckFailedException);
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : putReviewByUser, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : putReviewByUser, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
				LOGGER.error("API : putReviewByUser, Item collection size limit  excceeded", 
						itemCollectionSizeLimitExceededException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : putReviewByUser, Internal Server error", 
						internalServerErrorException);
			}
			return false;
		}
	
		/**
		 * Tested. Is Review Accessed By User i.e it ishelpful for user
		 * @param userid
		 * @param reviewid
		 * @return
		 */
	public boolean isReviewAccessedByUser(final String userid, final String reviewid) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
		checkArgument(StringUtils.isNotBlank(reviewid), "Reviewid must not be blank");

		try{
		  Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		  key.put("country", new AttributeValue().withS("INDIA"));
		  key.put("reviewid_userid", new AttributeValue().withS(reviewid + "_" + userid));
	      final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		  final Map<String, AttributeValue> items = getItemResult.getItem();
		  if(items != null) {
			  return true;
		  }
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : IsReviewAccessedByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : IsReviewAccessedByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : IsReviewAccessedByUser, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. Deletes table's row
	 * @param userid_reviewid
	 * @return
	 */
		public boolean deleteUerToReviewItem(final String userid, final String reviewid) {
			checkArgument(StringUtils.isNotBlank(userid), "userid must not be blank");
			checkArgument(StringUtils.isNotBlank(reviewid), "ReviewId must not be blank");
			try {
			   HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			   key.put("country", new AttributeValue().withS("INDIA"));
			   key.put("reviewid_userid", new AttributeValue().withS(reviewid + "_" + userid));

			   final ReturnValue returnValues = ReturnValue.ALL_OLD;
			   final DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
					.withTableName(TABLE_NAME).withKey(key).withReturnValues(returnValues);
			   if(deleteItemRequest == null) {
				   return false;
			   }
			   final DeleteItemResult result = amazonDynamoDBClient.deleteItem(deleteItemRequest);

			   // Check the response.
			   LOGGER.info("Printing item that was deleted...");
			   TableHelper.printItem(result.getAttributes());
			   return true;
			} 
			catch(ConditionalCheckFailedException conditionalCheckFailedException) {
				LOGGER.error("API : deleteUerToReviewItem, Error while condition checks", 
						conditionalCheckFailedException);
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : deleteUerToReviewItem, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : deleteUerToReviewItem, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
				LOGGER.error("API : deleteUerToReviewItem, Item collection size limit  excceeded", 
						itemCollectionSizeLimitExceededException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : deleteUerToReviewItem, Internal Server error", 
						internalServerErrorException);
			}
			catch(NullPointerException nullPointerException) {
				LOGGER.error("API : deleteUerToReviewItem, NullPointerException possibly because of bad parameter", 
					nullPointerException);
			}
			return false;
		}
		
		/**
		 * Gets Reviews Accessed By user
		 * @param userid
		 * @return
		 */
	public java.util.List<ReviewId> getReviewsAccessedByUser(final String userid) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
		java.util.List<ReviewId> reviews = new ArrayList<ReviewId>();	
		try { 
			  final QuerySpec spec = new QuerySpec()
				.withKeyConditionExpression("country = :v_id and contains(reviewid_userid, :r_id)")
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
		        ReviewId reviewId  = new ReviewId(item.get("reviewid").toString());
		        reviews.add(reviewId);
		      }
		      return reviews;
		}
		catch(Exception exception) {
			LOGGER.error("API : getReviewsAccessedByUser, Exception thrown while querying for all Reviews accessed"
					+ "by user", 
					exception);
		}
		
		return null;
	}

	public static void main(String[] args) {
		try {
			
			final UserToReviewClient dbClient = new UserToReviewClient();
			
			//dbClient.createTable();
			System.out.println(dbClient.putReviewByUser("varunkohade.iitr@gmail.com", 
					"varunkohade.iitr@gmail.com_INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			//System.out.println(dbClient.putReviewByUser("nitish.kohade@gmail.com", 
			//		"varunkohade.iitr@gmail.com_INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			//System.out.println(dbClient.deleteUerToReviewItem("varunkohade.iitr@gmail.com", 
			//		"varunkohade.iitr@gmail.com_INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}