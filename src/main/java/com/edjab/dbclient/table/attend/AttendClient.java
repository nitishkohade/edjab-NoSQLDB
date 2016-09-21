//@Copyright Edjab 2016

package com.edjab.dbclient.table.attend;

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
import com.edjab.model.Attend;
import com.edjab.model.InstituteName;

/**
 * This is the Attend Client Class whose act as the application class for all the retrieval, updates w.r.t NoSqlDB
 * @author varun
 *
 */
public class AttendClient {
	
	private static final Logger LOGGER = Logger.getLogger(AttendClient.class);
	private static final String TABLE_NAME = "AttendEdjabProd";
	private final AmazonDynamoDBClient amazonDynamoDBClient;
	private final SchoolProfileClient schoolProfileClient;
	private final UserProfileClient userProfileClient;
	
	/**
	 * Default Constructor
	 * 1) Do some testing by adding client configuration and setting maxRetry method also see max connection method.
	 * It is not thread safe. this applies to all classes.
	 * @throws IOException
	 */
	public AttendClient() throws IOException {
		BasicConfigurator.configure();
		AWSCredentials credentials;
		//try it
		//ClientConfiguration clientConfiguration = new ClientConfiguration().withMaxErrorRetry(10);
		try {
		       credentials= new PropertiesCredentials(
				AttendClient.class
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
		//amazonDynamoDBClient = new AmazonDynamoDBClient(credentials, clientConfiguration);
    }

	/**
	 * Constructor for Dependency Injection
	 * @param dynamoDBClient
	 * @throws IOException
	 */
	public AttendClient(final AmazonDynamoDBClient dynamoDBClient) throws IOException {
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
	 * Tested. This function triggers backend lambda process. when user clicks on attend button then both 
	 * UserProfileTable and SchoolProfileTable gets updated.
	 * @param attendedBy
	 * @param attendedTo
	 * @return
	 */
	public boolean createAttendedByUser(final String attendedBy, final String attendedTo) {
		checkArgument(StringUtils.isNotBlank(attendedBy), "AttendedBy must not be blank");
		checkArgument(StringUtils.isNotBlank(attendedTo), "AttendedTo must not be blank");
		if(schoolProfileClient.isValidSchoolProfile(attendedTo) == false) {
			return false;
		}
		if(userProfileClient.IsValidUserProfile(attendedBy) == false) {
			return false;
		}
		try {
		final Attend attend = new Attend(attendedBy, 
				attendedTo, new DateTime(DateTimeZone.UTC ));
		putItems(attend);
		return true;
		}
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : createAttendedByUser, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : createAttendedByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : createAttendedByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : createAttendedByUser, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : createAttendedByUser, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. Has User attended University
	 * @param attendedBy
	 * @param attendedTo
	 * @return
	 */
	public boolean isAttendedByUser(final String attendedBy, final String attendedTo) {
		checkArgument(StringUtils.isNotBlank(attendedBy), "AttendedBy must not be blank");
		checkArgument(StringUtils.isNotBlank(attendedTo), "AttendedTo must not be blank");
		try {	
			Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("country", new AttributeValue().withS("INDIA"));
			key.put("userid_schoolnameid", new AttributeValue().withS(attendedBy + "_" + attendedTo));
			final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
			final Map<String, AttributeValue> items = getItemResult.getItem();
			if(items != null) {
		    	return true;
			}
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : isAttendedByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : isAttendedByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : isAttendedByUser, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. putting all necessary items in the table
	 * @param attend
	 */
	private void putItems(final Attend attend) {
		
		LOGGER.info("Putting items into table " + TABLE_NAME);
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		
		item.put("country", new AttributeValue().withS("INDIA"));
		item.put("userid_schoolnameid", new AttributeValue().withS(attend.getAttendedBy() + 
				"_" + attend.getAttendedTo()));
		item.put("attendedOn", new AttributeValue().withS(attend.getAttendedOn().toString()));
		item.put("schoolnameid", new AttributeValue().withS(attend.getAttendedTo()));
		item.put("userid", new AttributeValue().withS(attend.getAttendedBy()));
		
		final PutItemRequest itemRequest = new PutItemRequest().withTableName(
				TABLE_NAME).withItem(item);
		amazonDynamoDBClient.putItem(itemRequest);
	}
	
	/**
	 * Tested. This function triggers lambda function. when user removes himself from attend row then both 
	 * UserProfileTable and SchoolProfileTable should be updated.
	 * @param attendedBy
	 * @param attendedTo
	 * @return
	 */
	public boolean deleteAttendedByUser(final String attendedBy, final String attendedTo) {
		checkArgument(StringUtils.isNotBlank(attendedBy), "AttendedBy must not be blank");
		checkArgument(StringUtils.isNotBlank(attendedTo), "AttendedTo must not be blank");
		if(schoolProfileClient.isValidSchoolProfile(attendedTo) == false) {
			return false;
		}
		if(userProfileClient.IsValidUserProfile(attendedBy) == false) {
			return false;
		}
		try {
			HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("country", new AttributeValue().withS("INDIA"));
			key.put("userid_schoolnameid", new AttributeValue().withS(attendedBy + "_" + attendedTo));

			final ReturnValue returnValues = ReturnValue.ALL_OLD;

			final DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
					.withTableName(TABLE_NAME).withKey(key).withReturnValues(returnValues);

			final DeleteItemResult result = amazonDynamoDBClient.deleteItem(deleteItemRequest);
			
			LOGGER.info("Printing item that was deleted...");
			TableHelper.printItem(result.getAttributes());
			return true;
			} 
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : deleteAttendedByUser, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : deleteAttendedByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : deleteAttendedByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : deleteAttendedByUser, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : deleteAttendedByUser, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. Get all schools attended by User
	 * @param userid
	 * @return
	 */
	public java.util.List<InstituteName> getAllSchoolAttendedByUser(final String userid) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
		java.util.List<InstituteName> attends = new ArrayList<InstituteName>();
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
		        attends.add(instituteName);
		      }
		      return attends;
		}
		catch(Exception exception) {
			LOGGER.error("API : getAllSchoolAttendedByUser, Exception thrown while querying for all schools", 
					exception);
		}
		return null;
	}
	
	public static void main(String[] args) {
		try {
				
			final AttendClient dbClient = new AttendClient();
			
			dbClient.createTable();
			
			//System.out.println(dbClient.createAttendedByUser("varunkohade.iitr@gmail.com", 
			//		"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			
			//System.out.println(dbClient.createAttendedByUser("nitish.kohade@gmail.com", 
			//		"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			
			//System.out.println(dbClient.deleteAttendedByUser("varunkohade.iitr@gmail.com", 
			//		"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			
		    //System.out.println(dbClient.getAllSchoolAttendedByUser("varunkohade.iitr@gmail.com"));
			System.out.println(dbClient.isAttendedByUser("varunkohade.iitr@gmail.com", 
							"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}