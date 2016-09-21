//@Copyright Edjab 2016

package com.edjab.dbclient.table.follow;

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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.edjab.dbclient.table.helper.TableHelper;
import com.edjab.dbclient.table.schoolprofile.SchoolProfileClient;
import com.edjab.dbclient.table.userprofile.UserProfileClient;
import com.edjab.model.Follow;
import com.edjab.model.InstituteName;

/**
 * This is the Follow Client Class whose act as the application class for all the retrieval, updates w.r.t NoSqlDB
 * @author varun
 *
 */
public class FollowClient {

	private static final Logger LOGGER = Logger.getLogger(FollowClient.class);
	private static final String TABLE_NAME = "FollowEdjabProd";
	private final AmazonDynamoDBClient amazonDynamoDBClient;
	private final SchoolProfileClient schoolProfileClient;
	private final UserProfileClient userProfileClient;
	
	
	/**
	 * Default Constructor
	 * 1) Do some testing by adding client configuration and setting maxRetry method also see max connection method.
	 * It is not thread safe. this applies to all classes.
	 * @throws IOException
	 */
	public FollowClient() throws IOException {
		BasicConfigurator.configure();
		AWSCredentials credentials;
		try {
		       credentials= new PropertiesCredentials(
	    		   FollowClient.class
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
		schoolProfileClient = new SchoolProfileClient(amazonDynamoDBClient);
		userProfileClient = new UserProfileClient(amazonDynamoDBClient);
    }
	
	/**
	 * Constructor for Dependency Injection
	 * @param dynamoDBClient
	 * @throws IOException
	 */
	public FollowClient(final AmazonDynamoDBClient dynamoDBClient) throws IOException {
		checkNotNull(dynamoDBClient, "dynamoDBClient must not be null");
		BasicConfigurator.configure();
		amazonDynamoDBClient = dynamoDBClient;
		schoolProfileClient = new SchoolProfileClient(dynamoDBClient);
		userProfileClient = new UserProfileClient(dynamoDBClient);
	}

	/**
	 * TODO: Tested. Change partition. Here making only one partition due to throughput constraint. 
	 * So country with value "INDIA" is chosen as partition key
	 * @throws Exception
	 */
	public void createTable() throws Exception {
		try {
			
			LOGGER.info("Creating table " + TABLE_NAME);
			ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(
					"country").withAttributeType("S"));
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(
					"userid_schoolnameid").withAttributeType("S"));

			ArrayList<KeySchemaElement> tableKeySchemaElement = new ArrayList<KeySchemaElement>();
			tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("country").withKeyType(
					KeyType.HASH));
			tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("userid_schoolnameid")
					.withKeyType(KeyType.RANGE));
			
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
	 * Tested. This function triggers backend lambda process. when user clicks on follow button then both 
	 * UserProfileTable and SchoolProfileTable gets updated. 
	 * @param followedBy
	 * @param followedTo
	 * @return
	 */
	public boolean createFollowedByUser(final String followedBy, final String followedTo) {
		checkArgument(StringUtils.isNotBlank(followedBy), "FollowedBy must not be blank");
		checkArgument(StringUtils.isNotBlank(followedTo), "FollowedTo must not be blank");
		if(schoolProfileClient.isValidSchoolProfile(followedTo) == false) {
			return false;
		}
		if(userProfileClient.IsValidUserProfile(followedBy) == false) {
			return false;
		}
		try {
		final Follow follow = new Follow(followedBy, 
				followedTo, new DateTime(DateTimeZone.UTC ));
		putItems(follow);
		return true;
		}
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : createFollowedByUser, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : createFollowedByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : createFollowedByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : createFollowedByUser, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : createFollowedByUser, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. Has User followed University
	 * @param followedBy
	 * @param followedTo
	 * @return
	 */
	public boolean isFollowedByUser(final String followedBy, final String followedTo) {
		checkArgument(StringUtils.isNotBlank(followedBy), "FollowedBy must not be blank");
		checkArgument(StringUtils.isNotBlank(followedTo), "FollowedTo must not be blank");
		try {	
			Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("country", new AttributeValue().withS("INDIA"));
			key.put("userid_schoolnameid", new AttributeValue().withS(followedBy + "_" + followedTo));
			final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
			final Map<String, AttributeValue> items = getItemResult.getItem();
			if(items != null) {
		    	return true;
			}
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : isFollowedByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : isFollowedByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : isFollowedByUser, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. putting all necessary items in the table
	 * @param follow
	 */
	private void putItems(final Follow follow) {
		
		LOGGER.info("Putting items into table " + TABLE_NAME);
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		
		item.put("country", new AttributeValue().withS("INDIA"));
		item.put("userid_schoolnameid", new AttributeValue().withS(follow.getFollowedBy() + 
				"_" + follow.getFollowedTo()));
		item.put("followedOn", new AttributeValue().withS(follow.getFollowedOn().toString()));
		item.put("schoolnameid", new AttributeValue().withS(follow.getFollowedTo()));
		item.put("userid", new AttributeValue().withS(follow.getFollowedBy()));
		
		final PutItemRequest itemRequest = new PutItemRequest().withTableName(
				TABLE_NAME).withItem(item);
		amazonDynamoDBClient.putItem(itemRequest);
	}
	
	/**
	 * Tested. This function triggers lambda function. when user removes himself from follow row then both 
	 * UserProfileTable and SchoolProfileTable should be updated.
	 * @param followedBy
	 * @param followedTo
	 * @return
	 */
	public boolean deleteFollowedByUser(final String followedBy, final String followedTo) {
		checkArgument(StringUtils.isNotBlank(followedBy), "FollowedBy must not be blank");
		checkArgument(StringUtils.isNotBlank(followedTo), "FollowedTo must not be blank");
		if(schoolProfileClient.isValidSchoolProfile(followedTo) == false) {
			return false;
		}
		if(userProfileClient.IsValidUserProfile(followedBy) == false) {
			return false;
		}
		try {
			HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("country", new AttributeValue().withS("INDIA"));
			key.put("userid_schoolnameid", new AttributeValue().withS(followedBy + "_" + followedTo));

			final ReturnValue returnValues = ReturnValue.ALL_OLD;

			final DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
					.withTableName(TABLE_NAME).withKey(key).withReturnValues(returnValues);

			final DeleteItemResult result = amazonDynamoDBClient.deleteItem(deleteItemRequest);
			
			LOGGER.info("Printing item that was deleted...");
			TableHelper.printItem(result.getAttributes());
			return true;
			} 
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : deleteFollowedByUser, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : deleteFollowedByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : deleteFollowedByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : deleteFollowedByUser, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : deleteFollowedByUser, Internal Server error", 
					internalServerErrorException);
		}
			return false;
		}
	
	/**
	 * Tested. Get all schools followed by User
	 * @param userid
	 * @return
	 */
	public java.util.List<InstituteName> getAllSchoolFollowedByUser(final String userid) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
		java.util.List<InstituteName> follows = new ArrayList<InstituteName>();
		try {
			final QuerySpec spec = new QuerySpec()
			.withKeyConditionExpression("country = :v_id and begins_with(userid_schoolnameid, :r_id)")
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
		        InstituteName instituteName = new InstituteName(item.get("schoolnameid").toString());
		        follows.add(instituteName);
		      }
		      return follows;
		}
		catch(Exception exception) {
			LOGGER.error("API : getAllSchoolFollowedByUser, Exception thrown while querying for all schools", 
					exception);
		}
		return null;
	}
	
	public static void main(String[] args) {
		try {
				
			final FollowClient dbClient = new FollowClient();
			
			//dbClient.createTable();
			
			//System.out.println(dbClient.createFollowedByUser("varunkohade.iitr@gmail.com", 
			//		"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			//System.out.println(dbClient.createFollowedByUser("nitish.kohade@gmail.com", 
			//		"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			
			//System.out.println(dbClient.deleteFollowedByUser("varunkohade.iitr@gmail.com", 
			//		"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			
		    System.out.println(dbClient.getAllSchoolFollowedByUser("varunkohade.iitr@gmail.com"));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}