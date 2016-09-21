//@Copyright Edjab 2016

package com.edjab.dbclient.table.review;

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
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
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
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.edjab.dbclient.table.helper.TableHelper;
import com.edjab.dbclient.table.schoolprofile.SchoolProfileClient;
import com.edjab.dbclient.table.userprofile.UserProfileClient;
import com.edjab.model.Review;

/**
 * This is the Review Client Class whose act as the application class for all the retrieval, 
 * updates w.r.t NoSqlDB.
 * @author varun
 *
 */
public class ReviewClient {

	private static final Logger LOGGER = Logger.getLogger(ReviewClient.class);
	private static final String TABLE_NAME = "ReviewEdjabProd";
	private static final String MOST_HELPFUL_REVIEWS_INDEX = "MostHelpfulReviewsBySchoolEdjabProd";
	private final AmazonDynamoDBClient amazonDynamoDBClient;
	private final SchoolProfileClient schoolProfileClient;
	private final UserProfileClient userProfileClient;
	
	/**
	 * Default Constructor
	 * 1) Do some testing by adding client configuration and setting maxRetry method also see max connection method.
	 * It is not thread safe. this applies to all classes.
	 * @throws IOException
	 */
	public ReviewClient() throws IOException {
		BasicConfigurator.configure();
		AWSCredentials credentials;
		try {
		       credentials= new PropertiesCredentials(
		    		   ReviewClient.class
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
	public ReviewClient(final AmazonDynamoDBClient dynamoDBClient) 
			throws IOException {
		checkNotNull(dynamoDBClient, "dynamoDBClient must not be null");
		BasicConfigurator.configure();
		amazonDynamoDBClient = dynamoDBClient;
		schoolProfileClient = new SchoolProfileClient(dynamoDBClient);
		userProfileClient = new UserProfileClient(dynamoDBClient);
	}
	
	/**
	 * TODO: Tested. Change Partition
	 */
	public void createTable() {
		try {
			LOGGER.info("Creating table " + TABLE_NAME);
			ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(
					"country").withAttributeType("S"));
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(
					"userid_schoolnameid").withAttributeType("S"));
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(
					"schoolnameid").withAttributeType("S"));
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(
					"helpfulVotes").withAttributeType("N"));


			ArrayList<KeySchemaElement> tableKeySchemaElement = new ArrayList<KeySchemaElement>();
			tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("country").withKeyType(
					KeyType.HASH));
			tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("userid_schoolnameid").withKeyType(
					KeyType.RANGE));
			
			final ProvisionedThroughput tableProvisionedThroughput = new ProvisionedThroughput()
			.withReadCapacityUnits(1L).withWriteCapacityUnits(1L);
			
			final ProvisionedThroughput indexProvisionedThroughput = new ProvisionedThroughput()
			.withReadCapacityUnits(2L).withWriteCapacityUnits(1L);
			
			StreamSpecification streamSpecification = new StreamSpecification();
	        streamSpecification.setStreamEnabled(true);
	        streamSpecification.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);

			//Most Helpful Reviews Index
			GlobalSecondaryIndex MostHelpfulIndex = new GlobalSecondaryIndex()
			.withIndexName(MOST_HELPFUL_REVIEWS_INDEX)
			.withProvisionedThroughput(indexProvisionedThroughput)
			.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
			
			ArrayList<KeySchemaElement> MostHelpfulReviewsindexKeySchemaElement = new ArrayList<KeySchemaElement>();

			MostHelpfulReviewsindexKeySchemaElement.add(new KeySchemaElement()
			    .withAttributeName("schoolnameid")
			    .withKeyType(KeyType.HASH));  //Partition key
			MostHelpfulReviewsindexKeySchemaElement.add(new KeySchemaElement()
			    .withAttributeName("helpfulVotes")
			    .withKeyType(KeyType.RANGE));  //Sort key

			MostHelpfulIndex.setKeySchema(MostHelpfulReviewsindexKeySchemaElement);
			
			final CreateTableRequest request = new CreateTableRequest()
					.withTableName(TABLE_NAME)
					.withAttributeDefinitions(attributeDefinitions)
					.withKeySchema(tableKeySchemaElement)
					.withProvisionedThroughput(tableProvisionedThroughput)
					.withGlobalSecondaryIndexes(MostHelpfulIndex)
					.withStreamSpecification(streamSpecification);

			final CreateTableResult result = amazonDynamoDBClient.createTable(request);
			LOGGER.info("Created table " + result.getTableDescription().getTableName());
		}
		catch(ResourceInUseException resourceInUseException) {
			LOGGER.info(TABLE_NAME + " has already been created");
		}
	}
	
	/**
	 * Tested. Creates Reviews By User
	 * @param reviewedBy
	 * @param reviewedTo
	 * @param reviewBody
	 * @param rating
	 * @return
	 */
		public boolean createReviewByUser(final String reviewedBy, final String reviewedTo, 
				final String reviewBody, final int rating) {
			checkArgument(StringUtils.isNotBlank(reviewedBy), "ReviewedBy field must not be blank");
			checkArgument(StringUtils.isNotBlank(reviewedTo), "ReviewedTo field must not be blank");
			checkArgument(StringUtils.isNotBlank(reviewBody), "ReviewBody field must not be blank");
			checkArgument(rating > 0 && rating <=5);
			if(schoolProfileClient.isValidSchoolProfile(reviewedTo) == false) {
				return false;
			}
			if(userProfileClient.IsValidUserProfile(reviewedBy) == false) {
				return false;
			}
			if(isReviewedByUser(reviewedTo, reviewedBy) == true) {
				System.out.println("Reviewed by user");
				return false;
			}
			
			try {
			final Review review = new Review(
					reviewedBy + "_" + reviewedTo, 
					reviewBody, 
					reviewedBy, 
					reviewedTo, 
					new DateTime(DateTimeZone.UTC), 
					0, 
					rating);
			putItems(review);
			return true;
			}
			catch(ConditionalCheckFailedException conditionalCheckFailedException) {
				LOGGER.error("API : createReviewByUser, Error while condition checks", 
						conditionalCheckFailedException);
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : createReviewByUser, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : createReviewByUser, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
				LOGGER.error("API : createReviewByUser, Item collection size limit  excceeded", 
						itemCollectionSizeLimitExceededException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : createReviewByUser, Internal Server error", 
						internalServerErrorException);
			}
			return false;
		}
	
		/**
		 * Tested. Is reviewed By user
		 * @param schoolnameid
		 * @param userid
		 * @return
		 */
	public boolean isReviewedByUser(final String schoolnameid, final String userid) {
		checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid field must not be blank");
		checkArgument(StringUtils.isNotBlank(userid), "Userid field must not be blank");
		try {	
			Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("country", new AttributeValue().withS("INDIA"));
			key.put("userid_schoolnameid", new AttributeValue().withS(userid + "_" + schoolnameid));
			final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
			final Map<String, AttributeValue> items = getItemResult.getItem();
			if(items != null) {
		    	return true;
			}
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : isReviewedByUser, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : isReviewedByUser, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : isReviewedByUser, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. Put Items
	 * @param review
	 */
		private void putItems(final Review review) {
			if(review == null) {
				return;
			}
			LOGGER.info("Putting items into table " + TABLE_NAME);
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
			String reviewBody = review.getReviewBody().toString();
			if(reviewBody.isEmpty()) {
				reviewBody = " ";
			}
			item.put("country", new AttributeValue().withS("INDIA"));
			item.put("userid_schoolnameid", new AttributeValue().withS(review.getReviewedBy() + "_" + 
					review.getReviewedTo()));
			item.put("reviewBody", new AttributeValue().withS(reviewBody));
			item.put("userid", new AttributeValue().withS(review.getReviewedBy()));
			item.put("reviewedOn", new AttributeValue().withS(review.getReviewedOn().toString()));
			item.put("schoolnameid", new AttributeValue().withS(review.getReviewedTo()));
			item.put("helpfulVotes", new AttributeValue().withN(review.getHelpfulVotes().toString()));
			item.put("ratedNumber", new AttributeValue().withN(review.getRatedNumber().toString()));
		
			final PutItemRequest itemRequest = new PutItemRequest().withTableName(
					TABLE_NAME).withItem(item);
			amazonDynamoDBClient.putItem(itemRequest);
		}
	
	/**
	 * Tested. Gets Review
	 * @param schoolnameid
	 * @param userid
	 * @return
	 */
	public Review getReview(final String schoolnameid, final String userid) {
		checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid field must not be blank");
		checkArgument(StringUtils.isNotBlank(userid), "Userid field must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("country", new AttributeValue().withS("INDIA"));
		key.put("userid_schoolnameid", new AttributeValue().withS(userid + "_" + schoolnameid));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		
		String reviewBody = "";
		String reviewedOn = "";
		String helpfulVotes = "";
		String ratedNumber = "";
		
		for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
				String attributeName = item.getKey();
				String value = "";
				if(attributeName.equals("reviewBody")) {
					value = item.getValue().getS();
					if(value != null) {
						reviewBody = value;
					}
				}
				if(attributeName.equals("reviewedOn")) {
					value = item.getValue().getS();
					if(value != null) {
						reviewedOn =  value;
					}
				}
				if(attributeName.equals("helpfulVotes")) {
					value = item.getValue().getN();
					if(value != null) {
						helpfulVotes =  value;
					}
				}
				if(attributeName.equals("ratedNumber")) {
					value = item.getValue().getN();
					if(value != null) {
						ratedNumber =  value;
					}
				}
		}
		return new Review(
				userid + "_" + schoolnameid, 
				reviewBody, 
				userid, 
				schoolnameid, 
				DateTime.parse(reviewedOn), 
				Integer.parseInt(helpfulVotes), 
				Integer.parseInt(ratedNumber));
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : getReview, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : getReview, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : getReview, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : getReview, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return null;
	}
	
	/**
	 * Tested. Is Review present
	 * @param reviewId
	 * @return
	 */
	public boolean isReview(final String reviewId) {
		checkArgument(StringUtils.isNotBlank(reviewId), "reviewId field must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("country", new AttributeValue().withS("INDIA"));
		key.put("userid_schoolnameid", new AttributeValue().withS(reviewId));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		
		if(items == null) {
			return false;
		}
		return true;
		
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : getReview, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : getReview, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : getReview, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. Delete Review
	 * @param schoolnameid
	 * @param userid
	 * @return
	 */
		public Boolean deleteReview(final String schoolnameid, final String userid) {
			checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid field must not be blank");
			checkArgument(StringUtils.isNotBlank(userid), "Userid field must not be blank");
			
			try {
			HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("country", new AttributeValue().withS("INDIA"));
			key.put("userid_schoolnameid", new AttributeValue().withS(userid + "_" + schoolnameid));

			final ReturnValue returnValues = ReturnValue.ALL_OLD;
			
			final DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
					.withTableName(TABLE_NAME).withKey(key).withReturnValues(returnValues);

			final DeleteItemResult result = amazonDynamoDBClient.deleteItem(deleteItemRequest);
			
			if(result.getAttributes() == null) {
				return false;
			}
			
			return true;
			} 
			catch(ConditionalCheckFailedException conditionalCheckFailedException) {
				LOGGER.error("API : deleteReview, Error while condition checks", 
						conditionalCheckFailedException);
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : deleteReview, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : deleteReview, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
				LOGGER.error("API : deleteReview, Item collection size limit  excceeded", 
						itemCollectionSizeLimitExceededException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : deleteReview, Internal Server error", 
						internalServerErrorException);
			}
			catch(NullPointerException nullPointerException) {
				LOGGER.error("API : deleteReview, NullPointerException possibly because of bad parameter", 
					nullPointerException);
			}
			return false;
		}
	
	/**
	 * Tested. Gets Reviews By User
	 * @param userid
	 * @return
	 */
	public java.util.List<Review> getReviewsByUser(final String userid) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid field must not be blank");
		java.util.List<Review> reviews = new ArrayList<Review>();	
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
		        Review review = new Review(
		        		item.get("userid_schoolnameid").toString(), 
		        		item.get("reviewBody").toString(), 
		        		item.get("userid").toString(), 
		        		item.get("schoolnameid").toString(), 
		        		DateTime.parse(item.get("reviewedOn").toString()), 
		        		Integer.parseInt(item.get("helpfulVotes").toString()), 
		        		Integer.parseInt(item.get("ratedNumber").toString()));
		        
		        reviews.add(review);
		      }
		      return reviews;
		}
		catch(Exception exception) {
			LOGGER.error("API : getReviewsByUser, Exception thrown while querying for all Reviews", 
					exception);
		}
		return null;
	}
	
	/**
	 * Tested. Gets Reviews By School
	 * @param schoolnameid
	 * @return
	 */
	public java.util.List<Review> getReviewsBySchool(final String schoolnameid) {
		checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid field must not be blank");
		java.util.List<Review> reviews = new ArrayList<Review>();	
		try {
			   final QuerySpec spec = new QuerySpec()
		      .withKeyConditionExpression("schoolnameid = :v_id")
		      .withValueMap(new ValueMap().withString(":v_id", schoolnameid));
			
			  final DynamoDB dynamoDB = new DynamoDB(amazonDynamoDBClient);
		      final Table table = dynamoDB.getTable(TABLE_NAME);
			  final Index index = table.getIndex(MOST_HELPFUL_REVIEWS_INDEX);
		      final ItemCollection<QueryOutcome> items = index.query(spec);
		
		      final Iterator<Item> iterator = items.iterator();
		      Item item = null;
		      while (iterator.hasNext()) 
		      {
		        item = iterator.next();
		        Review review = new Review(
		        		item.get("userid_schoolnameid").toString(), 
		        		item.get("reviewBody").toString(), 
		        		item.get("userid").toString(), 
		        		item.get("schoolnameid").toString(), 
		        		DateTime.parse(item.get("reviewedOn").toString()), 
		        		Integer.parseInt(item.get("helpfulVotes").toString()), 
		        		Integer.parseInt(item.get("ratedNumber").toString()));
		        reviews.add(review);
		      }
		      return reviews;
		}
		catch(Exception exception) {
			LOGGER.error("API : getReviewsBySchool, Exception thrown while querying for all Videos", 
					exception);
		}
		return null;
		
	}
	
	/**
	 * Tested. Edit Review Item
	 * @param schoolnameid
	 * @param userid
	 * @param reviewBody
	 * @param updatedRating
	 * @return
	 */
	public Boolean editReview(String schoolnameid, String userid, String reviewBody, int updatedRating) {
		checkArgument(StringUtils.isNotBlank(userid), "Userid field must not be blank");
		checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid field must not be blank");
		checkArgument(StringUtils.isNotBlank(reviewBody), "ReviewBody field must not be blank");
		checkArgument(updatedRating > 0 && updatedRating <=5);
		
		if(schoolProfileClient.isValidSchoolProfile(schoolnameid) == false) {
			return false;
		}
		if(userProfileClient.IsValidUserProfile(userid) == false) {
			return false;
		}
		if(isReview(userid + "_" + schoolnameid) == false) {
			return false;
		}
		
		try {
			Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
			Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("country", new AttributeValue().withS("INDIA"));
			key.put("userid_schoolnameid", new AttributeValue().withS(userid + "_" + schoolnameid));
			if(reviewBody.isEmpty()) {
				reviewBody = " ";
			}
			updateItems
				.put("reviewBody",
						new AttributeValueUpdate().withAction(
								AttributeAction.PUT).withValue(
										new AttributeValue().withS(reviewBody)));
			
			updateItems
				.put("ratedNumber",
						new AttributeValueUpdate().withAction(
								AttributeAction.PUT).withValue(
										new AttributeValue().withN(""+updatedRating)));
			
			updateItems
			.put("reviewedOn",
					new AttributeValueUpdate().withAction(
							AttributeAction.PUT).withValue(
									new AttributeValue().withS(new DateTime(DateTimeZone.UTC).toString())));
			
			final ReturnValue returnValues = ReturnValue.ALL_NEW;
					
			final UpdateItemRequest updateItemRequest = new UpdateItemRequest()
			.withTableName(TABLE_NAME).withKey(key)
			.withAttributeUpdates(updateItems)
			.withReturnValues(returnValues);

			final UpdateItemResult result = amazonDynamoDBClient.updateItem(updateItemRequest);
			  // Check the response.
			  LOGGER.info("Printing item after attribute update...");
			  TableHelper.printItem(result.getAttributes());
			  return true;      
			}
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : editReview, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : editReview, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : editReview, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : editReview, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : editReview, Internal Server error", 
					internalServerErrorException);
		}
		return false;
    }
	
	public static void main(String[] args) {
		try {
			ReviewClient dbClient = new ReviewClient();
			
			//dbClient.createTable();
			
			//System.out.println(dbClient.getReviewsByUser("varunkohade.iitr@gmail.com"));
			//System.out.println(dbClient.getReviewsBySchool("INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND"));
			//System.out.println(dbClient.deleteReview("INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
			//		"varunkohade.iitr@gmail.com"));
			System.out.println(dbClient.createReviewByUser("varunkohade.iitr@gmail.com", 
					"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", "review", 4));
			//System.out.println(dbClient.createReviewByUser("nitish.kohade@gmail.com", 
			//		"INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", "review", 5));
			//System.out.println(dbClient.isReviewedByUser("dfdsf", "sdddsf"));
			//System.out.println(dbClient.getReview("INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
			//		"varunkohade.iitr@gmail.com"));
			//System.out.println(dbClient.editReview("INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
			//				"varunkohade.iitr@gmail.com", "hello", 3));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}