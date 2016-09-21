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
import com.edjab.dbclient.table.video.VideoClient;
import com.edjab.model.VideoUrl;

/**
 * This is the UserToVideo Client Class whose act as the application class for all the retrieval, 
 * updates w.r.t NoSqlDB
 * @author varun
 *
 */
public class UserToVideoClient {

	private static final Logger LOGGER = Logger.getLogger(UserToVideoClient.class);
	private static final String TABLE_NAME = "UserToVideoEdjabProd";
	private final AmazonDynamoDBClient amazonDynamoDBClient;
	private final UserProfileClient userProfileClient;
	private final VideoClient videoClient;

	/**
	 * Default Constructor
	 * 1) Do some testing by adding client configuration and setting maxRetry method also see max connection method.
	 * It is not thread safe. this applies to all classes.
	 * @throws IOException
	 */
	public UserToVideoClient() throws IOException {
		BasicConfigurator.configure();
		AWSCredentials credentials;
		try {
		       credentials= new PropertiesCredentials(
		    		   UserToVideoClient.class
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
		videoClient = new VideoClient(amazonDynamoDBClient);
	}
	
	/**
	 * Constructor for Dependency Injection
	 * @param dynamoDBClient
	 * @throws IOException
	 */
	public UserToVideoClient(final AmazonDynamoDBClient dynamoDBClient) throws IOException {
		checkNotNull(dynamoDBClient, "dynamoDBClient must not be null");
		BasicConfigurator.configure();
		amazonDynamoDBClient = dynamoDBClient;
		userProfileClient = new UserProfileClient(dynamoDBClient);
		videoClient = new VideoClient(dynamoDBClient);
	}
	
	/**
	 * TODO: Tested. Change partition. Here making only one partition due to throughput constraint. 
	 * So country with value "INDIA" is chosen as partition key
	 */
	public void createTable() {
		try {
			
		LOGGER.info("Creating table " + TABLE_NAME);
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"country").withAttributeType("S"));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"videoid_userid").withAttributeType("S"));


		ArrayList<KeySchemaElement> tableKeySchemaElement = new ArrayList<KeySchemaElement>();
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("country").withKeyType(
				KeyType.HASH));
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("videoid_userid").withKeyType(
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
	 * Tested Puts Video by User Items
	 * @param userid
	 * @param videoid
	 * @param videourl
	 * @param ishelpful
	 * @return
	 */
	public boolean putVideoByUser(final String userid, final String videoid, final String  videourl) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
		checkArgument(StringUtils.isNotBlank(videoid), "Videoid must not be blank");
		checkArgument(StringUtils.isNotBlank(videourl), "Videourl must not be blank");
		if(userProfileClient.IsValidUserProfile(userid) == false) {
			return false;
		}
		if(videoClient.getVideo(videoid) == null) {
			return false;
		}
		if(isVideoAccessedByUser(userid, videoid) == true) {
			return false;
		}
			try {
				  LOGGER.info("Putting items into table " + TABLE_NAME);
				  Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
				  item.put("country", new AttributeValue().withS("INDIA"));
				  item.put("videoid_userid", new AttributeValue().withS(videoid + "_" + userid));
				  item.put("videoid", new AttributeValue().withS(videoid));
				  item.put("videourl", new AttributeValue().withS(videourl));
				  
				  final PutItemRequest itemRequest = new PutItemRequest().withTableName(
						TABLE_NAME).withItem(item);
				  amazonDynamoDBClient.putItem(itemRequest);   			
			      return true;
			}
			catch(ConditionalCheckFailedException conditionalCheckFailedException) {
				LOGGER.error("API : putVideoByUser, Error while condition checks", 
						conditionalCheckFailedException);
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : putVideoByUser, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : putVideoByUser, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
				LOGGER.error("API : putVideoByUser, Item collection size limit  excceeded", 
						itemCollectionSizeLimitExceededException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : putVideoByUser, Internal Server error", 
						internalServerErrorException);
			}
			return false;
		}

	/**
	 * Tested. Is video accessed by user i.e it ishelpful for user
	 * @param userid
	 * @param videoid
	 * @return
	 */
	public boolean isVideoAccessedByUser(final String userid, final String videoid) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
		checkArgument(StringUtils.isNotBlank(videoid), "Videoid must not be blank");
		 try{
		  Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		  key.put("country", new AttributeValue().withS("INDIA"));
		  key.put("videoid_userid", new AttributeValue().withS(videoid + "_" + userid));
	      final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		  final Map<String, AttributeValue> items = getItemResult.getItem();
		  if(items != null) {
			  return true;
		  }	
		 }
		 catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : IsVideoAccessedByUser, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : IsVideoAccessedByUser, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : IsVideoAccessedByUser, Internal Server error", 
						internalServerErrorException);
			}
		  return false;
	}
	
	/**
	 * Tested. Deletes table's row
	 * @param userid
	 * @param videoid
	 * @return
	 */
	public boolean deleteUserToVideoItem(final String userid, final String videoid) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
		checkArgument(StringUtils.isNotBlank(videoid), "Videoid must not be blank");
		try {
		   HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		   key.put("country", new AttributeValue().withS("INDIA"));
		   key.put("videoid_userid", new AttributeValue().withS(videoid + "_" + userid));

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
			LOGGER.error("API : deleteUserToVideoItem, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : deleteUserToVideoItem, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : deleteUserToVideoItem, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : deleteUserToVideoItem, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : deleteUserToVideoItem, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : deleteUserToVideoItem, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return false;
	}
	
	/**
	 * Gets Videos Accessed By user
	 * @param userid
	 * @return
	 */
	public java.util.List<VideoUrl> getVideosAccessedByUser(final String userid) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid must not be blank");
		java.util.List<VideoUrl> videos = new ArrayList<VideoUrl>();	
		try { 
			  final QuerySpec spec = new QuerySpec()
				.withKeyConditionExpression("country = :v_id and contains(videoid_userid, :r_id)")
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
		        VideoUrl videoUrl = new VideoUrl(item.get("videourl").toString());
		        videos.add(videoUrl);
		      }
		      return videos;
		}
		catch(Exception exception) {
			LOGGER.error("API : getVideosAccessedByUser, Exception thrown while querying for all Videos accessed"
					+ "by user", 
					exception);
		}
		
		return null;
	}
	
	public static void main(String[] args) {
		try {
			final UserToVideoClient dbClient = new UserToVideoClient();
			
			//dbClient.createTable();
			System.out.println(dbClient.putVideoByUser("varunkohade.iitr@gmail.com", "537929d5-127b-4efc-9d54-9556be4e8284", 
					"//www.youtube.com/embed/JpvaOab6RJc"));
			//System.out.println(dbClient.deleteUserToVideoItem("varunkohade.iitr@gmail.com", 
			//		"6854c70d-10df-4ea0-902c-5f5d74f15f8a"));
			//System.out.println(dbClient.getVideosAccessedByUser("varunkohade.iitr@gmail.com"));
			//System.out.println(dbClient.isVideoAccessedByUser("varunkohade.iitr@gmail.com", 
			//		"0749092d-cfff-4c40-ab37-5e03a9bdd224"));
			//System.out.println(dbClient.getVideosAccessedByUser("varunkohade.iitr@gmail.com"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}