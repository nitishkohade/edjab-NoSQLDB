//@Copyright Edjab 2016
package com.edjab.dbclient.table.userprofile;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
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
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.edjab.dbclient.table.helper.TableHelper;
import com.edjab.model.DateOfBirth;
import com.edjab.model.EditableUserProfile;
import com.edjab.model.Frequency;
import com.edjab.model.Gender;
import com.edjab.model.IndianState;
import com.edjab.model.PhoneNumber;
import com.edjab.model.UserProfile;
import com.edjab.model.VisibleUserProfile;

/**
 * This is the User Profile Client Class whose act as the application class for all the retrieval, 
 * updates w.r.t NoSqlDB
 * @author varun
 *
 */
public class UserProfileClient {
	
	private static final Logger LOGGER = Logger.getLogger(UserProfileClient.class);
	private static final String TABLE_NAME = "UserProfileEdjabProd";
	private final AmazonDynamoDBClient amazonDynamoDBClient;

	/**
	 * Default Constructor
	 * 1) Do some testing by adding client configuration and setting maxRetry method also see max connection method.
	 * It is not thread safe. this applies to all classes.
	 * @throws IOException
	 */
	public UserProfileClient() throws IOException {
		BasicConfigurator.configure();
		AWSCredentials credentials;
		try {
		       credentials= new PropertiesCredentials(
		    		   UserProfileClient.class
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
	public UserProfileClient(final AmazonDynamoDBClient dynamoDBClient) throws IOException {
		checkNotNull(dynamoDBClient, "dynamoDBClient must not be null");
		BasicConfigurator.configure();
		amazonDynamoDBClient = dynamoDBClient;
	}
	
	/**
	 * Tested. Creates Table
	 */
	public void createTable() {
		try {
		LOGGER.info("Creating table " + TABLE_NAME);
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"userId").withAttributeType("S"));

		ArrayList<KeySchemaElement> keySchemaElement = new ArrayList<KeySchemaElement>();
		keySchemaElement.add(new KeySchemaElement().withAttributeName("userId").withKeyType(
				KeyType.HASH));

		final ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
				.withReadCapacityUnits(1L).withWriteCapacityUnits(1L);
		
		StreamSpecification streamSpecification = new StreamSpecification();
        streamSpecification.setStreamEnabled(true);
        streamSpecification.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);

		final CreateTableRequest request = new CreateTableRequest()
				.withTableName(TABLE_NAME)
				.withAttributeDefinitions(attributeDefinitions)
				.withKeySchema(keySchemaElement)
				.withProvisionedThroughput(provisionedThroughput)
				.withStreamSpecification(streamSpecification);

		final CreateTableResult result = amazonDynamoDBClient.createTable(request);
		LOGGER.info("Created table " + result.getTableDescription().getTableName());
		}
		catch(ResourceInUseException resourceInUseException) {
			LOGGER.info(TABLE_NAME + " has already been created");
		}
	}
	
    /**
     * Tested. Creates User Profile
     * @param userId
     * @param password
     * @param validRegisteredUser
     * @param validFacebookUser
     * @param validGoogleUser
     * @return
     */
	public boolean createUserProfile(final String userId, final String password, final boolean validRegisteredUser, 
			final boolean validFacebookUser, final boolean validGoogleUser) {
		
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		
		if(!isUserIdAvailable(userId)) {
			return false; 
		}
		
		if(validFacebookUser == false && validGoogleUser == false && validRegisteredUser == false) {
			return false;
		}
		
		if(validFacebookUser) {
			if(validGoogleUser || validRegisteredUser) {
				return false;
			}
		}
		
		if(validGoogleUser && validRegisteredUser) {
				return false;
		}
		
		String encryptedPassword = null;
		String emailId = null;
		
		if(validRegisteredUser == true) {
			checkArgument(StringUtils.isNotBlank(password), "Password must not be blank");
			encryptedPassword = TableHelper.getEncryptPasswordMD5(password);
			emailId = userId;
		}
		
		try {
		final UserProfile userProfile = 
				new UserProfile(null, null, null, emailId, userId, encryptedPassword, false, validFacebookUser, 
						validGoogleUser, null, null, null, null, null, null, null, 0, 0, 0, null, Frequency.WEEKLY, null, 
						0, null, null, null, null);
		putItems(userProfile);
		return true;
		}
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : createUserProfile, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : createUserProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : createUserProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : createUserProfile, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : createUserProfile, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. putting all necessary items in the table
	 * @param userProfile
	 */
	private void putItems(final UserProfile userProfile) {
		
		LOGGER.info("Putting items into table " + TABLE_NAME);
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		
		item.put("userId", new AttributeValue().withS(userProfile.getUserId()));
		item.put("validRegisteredUser", new AttributeValue().withS(userProfile.getRegisteredUser().toString().toUpperCase()));
		item.put("validFacebookUser", new AttributeValue().withS(userProfile.getFacebookUser().toString().toUpperCase()));
		item.put("validGoogleUser", new AttributeValue().withS(userProfile.getGoogleUser().toString().toUpperCase()));
		
		final String password = userProfile.getPassword();
		if(password != null) {
			item.put("password", new AttributeValue().withS(password));
		}
		else {
			item.put("password", new AttributeValue().withS(" "));
		}
		
		final String emailId = userProfile.getEmailId();
		if(emailId != null) {
			item.put("emailId", new AttributeValue().withS(emailId));
		}
		else {
			item.put("emailId", new AttributeValue().withS(" "));
		}
		
		final String registrationToken = userProfile.getRegistrationToken();
		if(registrationToken != null) {
			item.put("registrationToken", new AttributeValue().withS(registrationToken));
		}
		else {
			item.put("registrationToken", new AttributeValue().withS(" "));
		}
		
		final String passwordResetToken = userProfile.getPasswordResetToken();
		if(passwordResetToken != null) {
		    item.put("passwordResetToken", new AttributeValue().withS(passwordResetToken));
		}
		else {
			item.put("passwordResetToken", new AttributeValue().withS(" "));
		}
		
		final DateTime passwordResetTokenCreationTime = userProfile.getPasswordResetTokenCreationTime();
		if(passwordResetTokenCreationTime != null) {
		    item.put("passwordResetTokenCreationTime", new AttributeValue().withS(passwordResetTokenCreationTime.toString()));
		}
		else {
			item.put("passwordResetTokenCreationTime", new AttributeValue().withS(" "));
		}
		
		final String firstName = userProfile.getFirstName();
		if(firstName != null) {
		    item.put("firstName", new AttributeValue().withS(firstName.toLowerCase()));
		}
		else {
			item.put("firstName", new AttributeValue().withS(" "));
		}
		
		final String middleName = userProfile.getMiddleName();
		if(middleName != null) {
			item.put("middleName", new AttributeValue().withS(middleName.toLowerCase()));
		}
		else {
			item.put("middleName", new AttributeValue().withS(" "));
		}

		final String lastName = userProfile.getLastName();
		if(lastName != null) {
			item.put("lastName", new AttributeValue().withS(lastName.toLowerCase()));
		}
		else {
			item.put("lastName", new AttributeValue().withS(" "));
		}
		
		final Gender gender = userProfile.getGender();
		if(gender != null) {
			item.put("gender", new AttributeValue().withS(gender.toString().toUpperCase()));
		}
		else {
			item.put("gender", new AttributeValue().withS(" "));
		}
		
		final Frequency frequency = userProfile.getSubscriptionFrequency();
		if(frequency != null) {
			item.put("subscriptionFrequency", new AttributeValue().withS(frequency.toString().toUpperCase()));
		}
		else {
			item.put("subscriptionFrequency", new AttributeValue().withS(" "));
		}
		
		final String street = userProfile.getStreet();
		if(street != null) {
			item.put("street", new AttributeValue().withS(street.toLowerCase()));
		}
		else {
			item.put("street", new AttributeValue().withS(" "));
		}

		final String city = userProfile.getCity();
		if(city != null) {
			item.put("city", new AttributeValue().withS(city.toLowerCase()));
		}
		else {
			item.put("city", new AttributeValue().withS(" "));
		}
		
		final IndianState state = userProfile.getIndianState();
		if(state != null) {
			item.put("indianState", new AttributeValue().withS(state.toString().toLowerCase()));
		}
		else {
			item.put("indianState", new AttributeValue().withS(" "));
		}
		
		final String zip = userProfile.getZip();
		if(zip != null) {
			item.put("zip", new AttributeValue().withS(zip));
		}
		else {
			item.put("zip", new AttributeValue().withS(" "));
		}
		
		final String country = userProfile.getCountry();
		if(country != null) {
			item.put("country", new AttributeValue().withS(country.toLowerCase()));
		}
		else {
			item.put("country", new AttributeValue().withS(" "));
		}
		
		final PhoneNumber contactNumber = userProfile.getContactNumber();
		if(contactNumber != null) {
			item.put("contactNumber", new AttributeValue().withS(contactNumber.toString()));
		}
		else {
			item.put("contactNumber", new AttributeValue().withS(" "));
		}
		
		final Integer reviews = userProfile.getReviews();
		if(reviews != null) {
			item.put("reviews", new AttributeValue().withN(reviews.toString()));
		}
		else {
			item.put("reviews", new AttributeValue().withN("0"));
		}
		
		final Integer almaMaters = userProfile.getAlmaMaters();
		if(almaMaters != null) {
			item.put("almaMaters", new AttributeValue().withN(almaMaters.toString()));
		}
		else {
			item.put("almaMaters", new AttributeValue().withN("0"));
		}
		
		final Integer likes = userProfile.getLikes();
		if(likes != null) {
			item.put("likes", new AttributeValue().withN(likes.toString()));
		}
		else {
			item.put("likes", new AttributeValue().withN("0"));
		}
		
		final Integer follows = userProfile.getFollows();
		if(follows != null) {
			item.put("follows", new AttributeValue().withN(follows.toString()));
		}
		else {
			item.put("follows", new AttributeValue().withN("0"));
		}
		
		final java.util.List<String> titles = userProfile.getTitles();
		if(titles != null)  {
			item.put("titles", new AttributeValue().withSS(titles));
		}
		else {
			item.put("titles", new AttributeValue().withSS(" "));
		}
		
		final DateOfBirth dateOfbirth = userProfile.getDateOfBirth();
		if(dateOfbirth != null) {
			item.put("dateOfbirth", new AttributeValue().withS(dateOfbirth.toString()));
		}
		else {
			item.put("dateOfbirth", new AttributeValue().withS(" "));
		}
		
		final DateTime accountStatusChangeDate = userProfile.getAccountStatusChangeDate();
		if(accountStatusChangeDate != null) {
			item.put("accountStatusChangeDate", new AttributeValue().withS(accountStatusChangeDate.toString()));
		}
		else {
			item.put("accountStatusChangeDate", new AttributeValue().withS(" "));
		}

		final PutItemRequest itemRequest = new PutItemRequest().withTableName(
				TABLE_NAME).withItem(item);
		
		amazonDynamoDBClient.putItem(itemRequest);
	}
	
	/**
	 * Deletes User Profile. Should never be used. Just deactivate the profile if user deletes the profile
	 * @param userId
	 * @param password
	 * @return
	 */
	/*
	private boolean deleteUserProfile(final String userId, final String password) {
		
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		checkArgument(StringUtils.isNotBlank(password), "Password must not be blank");
		try {
		Map<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue>();
		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("userId", new AttributeValue().withS(userId));

		expectedValues.put("password", new ExpectedAttributeValue()
				.withValue(new AttributeValue().withS(TableHelper.getEncryptPasswordMD5(password))));

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
			LOGGER.error("API : deleteUserProfile, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : deleteUserProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : deleteUserProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : deleteUserProfile, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : deleteUserProfile, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	*/
	
	/**
	 * Tested. Is userId available to use by new user. It will return true when userid is not present in table
	 * @param userId
	 * @return
	 */
	public boolean isUserIdAvailable(final String userId) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		try {
		       Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		       key.put("userId", new AttributeValue().withS(userId));
		       final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		       final Map<String, AttributeValue> items = getItemResult.getItem();
		       if(items !=  null) {
		    	   	return false;
		       }
		       else {
		    	   return true;
		       }
			}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : isEmailIdAvailable, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : isEmailIdAvailable, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : isEmailIdAvailable, Internal Server error", 
					internalServerErrorException);
		}
		return false;
		}
	
	/**
	 * Tested. Gets User Profile
	 * @param userId
	 * @return
	 */
	public VisibleUserProfile getUserProfile(final String userId) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
			try{
			Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("userId", new AttributeValue().withS(userId));
			final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
			final Map<String, AttributeValue> items = getItemResult.getItem();
		
			String firstName = "";
			String middleName = "";
			String lastName = "";
			String email = "";
			int reviews = 0;
			int almamaters = 0;
			int likes = 0;
			int follows = 0;
			java.util.List<String> titles = new ArrayList<String>();
		    String city = "";
			String contactNumber = "";
			String country = "";
			String state = "";
			String street = "";
			String dateOfBirth = "";
			String gender = "";
			String zip = "";
			String freq = "";
			
			for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
					String attributeName = item.getKey();
					String value = "";
					
					if(attributeName.equals("dateOfBirth")) {
						value = item.getValue().getS();
						if(value != null) {
							dateOfBirth =  value;
						}
					}
					if(attributeName.equals("gender")) {
						value = item.getValue().getS();
						if(value != null) {
							gender = value;
						}
					}
					if(attributeName.equals("subscriptionFrequency")) {
						value = item.getValue().getS();
						if(value != null) {
							freq = value;
						}
					}
					if(attributeName.equals("firstName")) {
						value = item.getValue().getS();
						if(value != null) {
							firstName =  value;
						}
					}
					if(attributeName.equals("middleName")) {
						value = item.getValue().getS();
						if(value != null) {
							middleName =  value;
						}
					}
					if(attributeName.equals("lastName")) {
						value = item.getValue().getS();
						if(value != null) {
							lastName =  value;
						}
					}
					if(attributeName.equals("country")) {
						value = item.getValue().getS();
						if(value != null) {
							country = value;
						}
					}
					if(attributeName.equals("city")) {
						value = item.getValue().getS();
						if(value != null) {
							city =  value;
						}
					}
					if(attributeName.equals("emailId")) {
						value = item.getValue().getS();
						if(value != null) {
							email =  value;
						}
					}
					if(attributeName.equals("street")) {
						value = item.getValue().getS();
						if(value != null) {
							street =  value;
						}
					}
					if(attributeName.equals("contactNumber")) {
						value = item.getValue().getS();
						if(value != null) {
							contactNumber =  value;
						}
					}
					if(attributeName.equals("state")) {
						value = item.getValue().getS();
						if(value != null) {
							state =  value;
						}
					}
					if(attributeName.equals("zip")) {
						value = item.getValue().getS();
						if(value != null) {
							zip =  value;
						}
					}
					if(attributeName.equals("reviews")) {
						value = item.getValue().getN();
						if(value != null) {
							reviews =  Integer.parseInt(value);
						}
					}
					if(attributeName.equals("likes")) {
						value = item.getValue().getN();
						if(value != null) {
							likes =  Integer.parseInt(value);
						}
					}
					if(attributeName.equals("follows")) {
						value = item.getValue().getN();
						if(value != null) {
							follows =  Integer.parseInt(value);
						}
					}
					if(attributeName.equals("almaMaters")) {
						value = item.getValue().getN();
						if(value != null) {
							almamaters =  Integer.parseInt(value);
						}
					}
					if(attributeName.equals("titles")) {
						java.util.List<String> listvalue = (java.util.List<String>) item.getValue().getSS();
						if(listvalue != null) {
							for(String itemString : listvalue) {
								titles.add(itemString);
							}
						}
					}
			}
			return new VisibleUserProfile(firstName, middleName, lastName, email, gender, street, city, 
					state, country, zip, contactNumber, almamaters, follows, likes, titles, freq, 
					dateOfBirth, reviews);
		    }
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : getUserProfile, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : getUserProfile, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : getUserProfile, Internal Server error", 
						internalServerErrorException);
			}
			catch(NullPointerException nullPointerException) {
				LOGGER.error("API : getUserProfile, NullPointerException possibly because of bad parameter", 
					nullPointerException);
			}
			return null;
		}
	
	/**
	 * Tested. This function is used to find out authenticity of the user after its validation flag is true. This function will
	 * not work in case of Facebook or Google User
	 * @param userId
	 * @param password
	 * @return
	 */
	public boolean IsValidUserProfile(final String userId, final String password) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		checkArgument(StringUtils.isNotBlank(password), "Password must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("userId", new AttributeValue().withS(userId));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		boolean validityFlag = false;
		boolean passwordMatchFlag = false;
		for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
				String attributeName = item.getKey();
				String value = "";
				
				if(attributeName.equals("validRegisteredUser")) {
					value = item.getValue().getS();
					if(value.toUpperCase().equals("TRUE")) {
						validityFlag = true;
					}
				}
				if(attributeName.equals("password")) {
					value = item.getValue().getS();
					if(value.equals(TableHelper.getEncryptPasswordMD5(password))) {
						passwordMatchFlag = true;
					}
				}
		}
		if(validityFlag == true && passwordMatchFlag == true) {
			return true;
		}
		else {
			return false;
		}
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : IsValidUserProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : IsValidUserProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : IsValidUserProfile, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : IsValidUserProfile, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return false;
	}

	/**
	 * Tested. This function is used to find out authenticity of the user
	 * @param userId
	 * @return
	 */
	public boolean IsValidUserProfile(final String userId) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("userId", new AttributeValue().withS(userId));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		
		for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
				String attributeName = item.getKey();
				String value = "";
				
				if(attributeName.equals("validRegisteredUser")) {
					value = item.getValue().getS();
					if(value.toUpperCase().equals("TRUE")) {
						return true;
					}
				}
				if(attributeName.equals("validFacebookUser")) {
					value = item.getValue().getS();
					if(value.toUpperCase().equals("TRUE")) {
						return true;
					}
				}
				if(attributeName.equals("validGoogleUser")) {
					value = item.getValue().getS();
					if(value.toUpperCase().equals("TRUE")) {
						return true;
					}
				}
				
		}
		return false;
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : IsValidUserProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : IsValidUserProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : IsValidUserProfile, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : IsValidUserProfile, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return false;
	}
	
	/**
	 * Tested. This function is used to find out authenticity of the user Registered through our normal registration process
	 * @param userId
	 * @return
	 */
	public boolean IsValidRegisteredUserProfile(final String userId) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("userId", new AttributeValue().withS(userId));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		
		for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
				String attributeName = item.getKey();
				String value = "";
				
				if(attributeName.equals("validRegisteredUser")) {
					value = item.getValue().getS();
					if(value.toUpperCase().equals("TRUE")) {
						return true;
					}
				}
				
		}
		return false;
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : IsValidUserProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : IsValidUserProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : IsValidUserProfile, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : IsValidUserProfile, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return false;
	}
	
	/**
	 * Tested. This function is used to find out authenticity of the FaceBook user
	 * @param userId
	 * @return
	 */
	public boolean IsValidFBUserProfile(final String userId) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("userId", new AttributeValue().withS(userId));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		
		for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
				String attributeName = item.getKey();
				String value = "";
				
				if(attributeName.equals("validFacebookUser")) {
					value = item.getValue().getS();
					if(value.toUpperCase().equals("TRUE")) {
						return true;
					}
				}
		}
		return false;
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : IsValidUserProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : IsValidUserProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : IsValidUserProfile, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : IsValidUserProfile, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return false;
	}
	
	/**
	 * Tested. This function is used to find out authenticity of the Google user
	 * @param userId
	 * @return
	 */
	public boolean IsValidGoogleUserProfile(final String userId) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("userId", new AttributeValue().withS(userId));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		
		for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
				String attributeName = item.getKey();
				String value = "";
				
				if(attributeName.equals("validGoogleUser")) {
					value = item.getValue().getS();
					if(value.toUpperCase().equals("TRUE")) {
						return true;
					}
				}
		}
		return false;
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : IsValidUserProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : IsValidUserProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : IsValidUserProfile, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : IsValidUserProfile, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return false;
	}
	
	/**
	 * Tested. This function is used to make user registered through normal process valid
	 * @param userId
	 * @param tokenid
	 * @return
	 */
	public boolean validateUserProfile(final String userId, final String tokenid) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		checkArgument(StringUtils.isNotBlank(tokenid), "RegistrationTokenid must not be blank");
		try {
			Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		    key.put("userId", new AttributeValue().withS(userId));
		    final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
			final Map<String, AttributeValue> items = getItemResult.getItem();
			
			for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
					String attributeName = item.getKey();
					String value = "";
					
					if(attributeName.equals("registrationToken")) {
						value = item.getValue().getS();
						if(value == null || !value.equals(TableHelper.getEncryptPasswordMD5(tokenid))) {
							return false;
						}
					}
			}
			updateItems
			.put("registrationToken",
					new AttributeValueUpdate().withAction(
							AttributeAction.PUT).withValue(
							new AttributeValue().withS(" ")));
			updateItems
					.put("validRegisteredUser",
							new AttributeValueUpdate().withAction(
									AttributeAction.PUT).withValue(
									new AttributeValue().withS("TRUE")));
			
			updateItems
			.put("accountStatusChangeDate",
					new AttributeValueUpdate().withAction(
							AttributeAction.PUT).withValue(
							new AttributeValue().withS(""+new org.joda.time.DateTime( org.joda.time.DateTimeZone.UTC ))));
			
			
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
			LOGGER.error("API : validateUserProfile, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : validateUserProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : validateUserProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : validateUserProfile, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : validateUserProfile, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : validateUserProfile, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return false;
	}
	
	/**
	 * Tested. Creates Password Reset Token
	 * @param userId
	 * @return
	 */
	public String createPasswordResetToken(final String userId) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		if(IsValidUserProfile(userId) == false) {
			return null;
		}
		try {
			Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		    key.put("userId", new AttributeValue().withS(userId));
		    String passwordRestToken = UUID.randomUUID().toString();
			updateItems
					.put("passwordResetToken",
							new AttributeValueUpdate().withAction(
									AttributeAction.PUT).withValue(
									new AttributeValue().withS(TableHelper.getEncryptPasswordMD5(passwordRestToken))));
			
			updateItems
			.put("passwordResetTokenCreationTime",
					new AttributeValueUpdate().withAction(
							AttributeAction.PUT).withValue(
							new AttributeValue().withS(""+new org.joda.time.DateTime( org.joda.time.DateTimeZone.UTC ))));
			
			
			final ReturnValue returnValues = ReturnValue.ALL_NEW;
			
			final UpdateItemRequest updateItemRequest = new UpdateItemRequest()
			.withTableName(TABLE_NAME).withKey(key)
			.withAttributeUpdates(updateItems)
			.withReturnValues(returnValues);

	         final UpdateItemResult result = amazonDynamoDBClient.updateItem(updateItemRequest);

	         // Check the response.
	         LOGGER.info("Printing item after attribute update...");
	        TableHelper.printItem(result.getAttributes());
		return passwordRestToken;
		}
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : createPasswordResetToken, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : createPasswordResetToken, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : createPasswordResetToken, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : createPasswordResetToken, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : createPasswordResetToken, Internal Server error", 
					internalServerErrorException);
		}
		return null;
	}

	/**
	 * Tested. Creates Registration token
	 * @param userId
	 * @return
	 */
	public String createRegistrationToken(final String userId) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		if(isUserIdAvailable(userId) == true) {
			return null;
		}
			try {
				Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
			    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			    key.put("userId", new AttributeValue().withS(userId));
			    String registrationToken = UUID.randomUUID().toString();
				updateItems
						.put("registrationToken",
								new AttributeValueUpdate().withAction(
										AttributeAction.PUT).withValue(
										new AttributeValue().withS(TableHelper.getEncryptPasswordMD5(registrationToken))));
				
				final ReturnValue returnValues = ReturnValue.ALL_NEW;
				
				final UpdateItemRequest updateItemRequest = new UpdateItemRequest()
				.withTableName(TABLE_NAME).withKey(key)
				.withAttributeUpdates(updateItems)
				.withReturnValues(returnValues);

		         final UpdateItemResult result = amazonDynamoDBClient.updateItem(updateItemRequest);

		         // Check the response.
		         LOGGER.info("Printing item after attribute update...");
		        TableHelper.printItem(result.getAttributes());
			return registrationToken;
			}
			catch(ConditionalCheckFailedException conditionalCheckFailedException) {
				LOGGER.error("API : createRegistrationToken, Error while condition checks", 
						conditionalCheckFailedException);
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : createRegistrationToken, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : createRegistrationToken, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
				LOGGER.error("API : createRegistrationToken, Item collection size limit  excceeded", 
						itemCollectionSizeLimitExceededException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : createRegistrationToken, Internal Server error", 
						internalServerErrorException);
			}
			return null;
		}
	
	/**
	 * Tested. This function is used to make user invalid or deactivate the account
	 * @param userId
	 * @return
	 */
	public boolean deactivateUserProfile(final String userId) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		if(IsValidUserProfile(userId)==false) {
			return false;
		}
		try {
			Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		    key.put("userId", new AttributeValue().withS(userId));
			updateItems
			.put("validRegisteredUser",
							new AttributeValueUpdate().withAction(
									AttributeAction.PUT).withValue(
									new AttributeValue().withS("FALSE")));
			updateItems
			.put("validFacebookUser",
					new AttributeValueUpdate().withAction(
							AttributeAction.PUT).withValue(
							new AttributeValue().withS("FALSE")));
			updateItems
			.put("validGoogleUser",
					new AttributeValueUpdate().withAction(
							AttributeAction.PUT).withValue(
							new AttributeValue().withS("FALSE")));
			updateItems
			.put("accountStatusChangeDate",
					new AttributeValueUpdate().withAction(
							AttributeAction.PUT).withValue(
							new AttributeValue().withS(""+new org.joda.time.DateTime( org.joda.time.DateTimeZone.UTC ))));
			
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
			LOGGER.error("API : deactivateUserProfile, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : deactivateUserProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : deactivateUserProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : deactivateUserProfile, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : deactivateUserProfile, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. This function is used to update the password of the user. This function will be used by user when he knows his password
	 * @param userId
	 * @param currentPassword
	 * @param updatedPassword
	 * @return
	 */
	public boolean updateUserPassword(final String userId, final String currentPassword,
			final String updatedPassword) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		checkArgument(StringUtils.isNotBlank(currentPassword), "CurrentPassword must not be blank");
		checkArgument(StringUtils.isNotBlank(updatedPassword), "UpdatedPassword must not be blank");
		try {
			Map<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue>();
			Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		    key.put("userId", new AttributeValue().withS(userId));
		    expectedValues.put("password", new ExpectedAttributeValue()
			.withValue(new AttributeValue().withS(TableHelper.getEncryptPasswordMD5(currentPassword))));
			updateItems
					.put("password",
							new AttributeValueUpdate().withAction(
									AttributeAction.PUT).withValue(
									new AttributeValue().withS(TableHelper.getEncryptPasswordMD5(updatedPassword))));
			
			final ReturnValue returnValues = ReturnValue.ALL_NEW;
			
			final UpdateItemRequest updateItemRequest = new UpdateItemRequest()
			.withTableName(TABLE_NAME).withKey(key).withExpected(expectedValues)
			.withAttributeUpdates(updateItems)
			.withReturnValues(returnValues);

	         final UpdateItemResult result = amazonDynamoDBClient.updateItem(updateItemRequest);

	         // Check the response.
	         LOGGER.info("Printing item after attribute update...");
	         TableHelper.printItem(result.getAttributes());
		return true;
		}
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : updateUserPassword, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : updateUserPassword, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : updateUserPassword, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : updateUserPassword, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : updateUserPassword, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}

	/**
	 * Tested. Update Subscription frequency
	 * @param userId
	 * @param freq
	 * @return
	 */
	public boolean updateSubscriptionFrequency(final String userId, final String freq) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		checkArgument(StringUtils.isNotBlank(freq), "Subscription Frequency must not be blank");
		if(IsValidUserProfile(userId) == false) {
			return false;
		}
		try {
			Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		    key.put("userId", new AttributeValue().withS(userId));
		    String updatedSubscriptionFrequency = "";
		    
		    if(freq.toUpperCase().equals("DAILY")) {
		    	updatedSubscriptionFrequency = Frequency.DAILY.toString();
		    }
		    else if(freq.toUpperCase().equals("WEEKLY")) {
		    	updatedSubscriptionFrequency = Frequency.WEEKLY.toString();
		    }
		    else if(freq.toUpperCase().equals("MONTHLY")) {
		    	updatedSubscriptionFrequency = Frequency.MONTHLY.toString();
		    }
		    else {
		    	updatedSubscriptionFrequency = Frequency.UNSUBSCRIBE.toString();
		    }
		    
			updateItems
					.put("subscriptionFrequency",
							new AttributeValueUpdate().withAction(
									AttributeAction.PUT).withValue(
									new AttributeValue().withS(updatedSubscriptionFrequency)));
			
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
			LOGGER.error("API : updateSubscriptionFrequency, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : updateSubscriptionFrequency, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : updateSubscriptionFrequency, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : updateSubscriptionFrequency, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : updateSubscriptionFrequency, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. This function is used to change the password of the user when user does not know password
	 * @param userId
	 * @param tokenid
	 * @param updatedPassword
	 * @return
	 */
	public Boolean changeUserPassword(final String userId, final String tokenid, final String updatedPassword) {
		checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		checkArgument(StringUtils.isNotBlank(tokenid), "Password Reset Tokenid must not be blank");
		checkArgument(StringUtils.isNotBlank(updatedPassword), "updatedPassword must not be blank");
		try {
			Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		    key.put("userId", new AttributeValue().withS(userId));
		    
		    final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
			final Map<String, AttributeValue> items = getItemResult.getItem();
			
			for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
					String attributeName = item.getKey();
					String value = "";
					
					if(attributeName.equals("passwordResetToken")) {
						value = item.getValue().getS();
						if(value == null || !value.equals(TableHelper.getEncryptPasswordMD5(tokenid))) {
							return false;
						}
					}
					if(attributeName.equals("passwordResetTokenCreationTime")) {
						value = item.getValue().getS();
						if(value == null) {
							return false;
						}
						final DateTime dateTimeNow = new org.joda.time.DateTime( org.joda.time.DateTimeZone.UTC );
						final DateTime createTime = DateTime.parse(value);
						final long difference = dateTimeNow.getMillis() - createTime.getMillis();
						if(difference > 86400000) {
							return false;
						}
					}
			}
			updateItems
					.put("password",
							new AttributeValueUpdate().withAction(
									AttributeAction.PUT).withValue(
									new AttributeValue().withS(TableHelper.getEncryptPasswordMD5(updatedPassword))));
			updateItems
			.put("passwordResetToken",
					new AttributeValueUpdate().withAction(
							AttributeAction.PUT).withValue(
							new AttributeValue().withS(" ")));
			updateItems
			.put("passwordResetTokenCreationTime",
					new AttributeValueUpdate().withAction(
							AttributeAction.PUT).withValue(
							new AttributeValue().withS(" ")));
			
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
			LOGGER.error("API : changeUserPassword, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : changeUserPassword, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : changeUserPassword, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : changeUserPassword, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : changeUserPassword, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : changeUserPassword, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return false;
	}
	
	/**
	 * Tested. Is parametric token matching with emailid's Password reset Token
	 * @param userId
	 * @param tokenid
	 * @return
	 */
    public boolean IsMatchingPasswordResetToken(final String userId, final String tokenid) {
    	checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		checkArgument(StringUtils.isNotBlank(tokenid), "Password Reset Tokenid must not be blank");
			try {
				Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
				key.put("userId", new AttributeValue().withS(userId));
				final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
				final Map<String, AttributeValue> items = getItemResult.getItem();
				
				for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
						String attributeName = item.getKey();
						String value = "";
						
						if(attributeName.equals("passwordResetToken")) {
							value = item.getValue().getS();
							if(value == null || !value.equals(TableHelper.getEncryptPasswordMD5(tokenid))) {
								return false;
							}
						}
				}
				return true;
			}
			catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
				LOGGER.error("API : IsMatchingPasswordResetToken, Could not meet with provisioned throughput", 
						provisionedThroughputExceededException);
			}
			catch(ResourceNotFoundException resourceNotFoundException) {
				LOGGER.error("API : IsMatchingPasswordResetToken, resource probably table not found", 
						resourceNotFoundException);
			}
			catch(InternalServerErrorException internalServerErrorException) {
				LOGGER.error("API : IsMatchingPasswordResetToken, Internal Server error", 
						internalServerErrorException);
			}
			catch(NullPointerException nullPointerException) {
				LOGGER.error("API : IsMatchingPasswordResetToken, NullPointerException possibly because of bad parameter", 
					nullPointerException);
			}
			return false;
		}
    
    /**
     * Tested. Is parametric token matching with emailid's Registration Token
     * @param userId
     * @param tokenid
     * @return
     */
    public boolean IsMatchingRegistrationToken(final String userId, final String tokenid) {
    	checkArgument(StringUtils.isNotBlank(userId), "UserId must not be blank");
		checkArgument(StringUtils.isNotBlank(tokenid), "Registration Tokenid must not be blank");
		try{
			Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("userId", new AttributeValue().withS(userId));
			final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
			final Map<String, AttributeValue> items = getItemResult.getItem();
			
			for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
					String attributeName = item.getKey();
					String value = "";
					if(attributeName.equals("registrationToken")) {
						value = item.getValue().getS();
						if(value == null || !value.equals(TableHelper.getEncryptPasswordMD5(tokenid))) {
							return false;
						}
					}
			}
			return true;
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : IsMatchingRegistrationToken, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : IsMatchingRegistrationToken, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : IsMatchingRegistrationToken, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : IsMatchingRegistrationToken, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return false;
	}
	
    /**
     * This function is used to update user Profile Information
     * @param editableUserProfile
     * @return
     */
	public boolean updateUserProfileInfo(final EditableUserProfile editableUserProfile) {
		checkNotNull(editableUserProfile, "EditableUserProfile must not be null");
		String userId = editableUserProfile.getUserId().toString();
		if(IsValidUserProfile(userId) == false) {
			return false;
		}
		try {
			Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		    key.put("userId", new AttributeValue().withS(userId));
			
			final String firstName = editableUserProfile.getFirstName();
			if(firstName != null) {
				updateItems.put("firstName", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(firstName.toLowerCase())));
			}
			else {
				updateItems.put("firstName", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String middleName = editableUserProfile.getMiddleName();
			if(middleName != null) {
				updateItems.put("middleName", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(middleName.toLowerCase())));
			}
			else {
				updateItems.put("middleName", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String lastName = editableUserProfile.getLastName();
			if(lastName != null) {
				updateItems.put("lastName", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(lastName.toLowerCase())));
			}
			else {
				updateItems.put("lastName", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String gender = editableUserProfile.getGender();
			if(gender != null) {
				updateItems.put("gender", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(gender.toUpperCase())));
			}
			else {
				updateItems.put("gender", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String street = editableUserProfile.getStreet();
			if(street != null) {
				updateItems.put("street", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(street.toLowerCase())));
			}
			else {
				updateItems.put("street", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String city = editableUserProfile.getCity();
			if(city != null) {
				updateItems.put("city", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(city.toLowerCase())));
			}
			else {
				updateItems.put("city", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String state = editableUserProfile.getIndianState();
			if(state != null) {
				updateItems.put("indianState", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(state.toUpperCase())));
			}
			else {
				updateItems.put("indianState", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String country = editableUserProfile.getCountry();
			if(country != null) {
				updateItems.put("country", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(country.toLowerCase())));
			}
			else {
				updateItems.put("country", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String zip = editableUserProfile.getZip();
			if(zip != null) {
				updateItems.put("zip", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(zip)));
			}
			else {
				updateItems.put("zip", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String contactNumber = editableUserProfile.getContactNumber();
			if(contactNumber != null) {
				updateItems.put("contactNumber", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(contactNumber)));
			}
			else {
				updateItems.put("contactNumber", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String dateOfbirth = editableUserProfile.getDateOfBirth();
			if(dateOfbirth != null) {
				updateItems.put("dateOfbirth", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(dateOfbirth)));
			}
			else {
				updateItems.put("dateOfbirth", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final String emailId = editableUserProfile.getEmailId();
			if(emailId != null) {
				updateItems.put("emailId", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(emailId)));
			}
			else {
				updateItems.put("emailId", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
						new AttributeValue().withS(" ")));
			}
			
			final java.util.List<String> titleList = editableUserProfile.getTitles();
			
			if(titleList != null)  {
			java.util.List<String> titles = new ArrayList<String>(titleList.size());
			for(final String items : titleList) {
				titles.add(items);
			}
			updateItems.put("titles", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
					new AttributeValue().withSS(titles)));
		    }
			
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
			LOGGER.error("API : updateUserProfileInfo, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : updateUserProfileInfo, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : updateUserProfileInfo, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : updateUserProfileInfo, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : updateUserProfileInfo, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	public static void main(String[] args) {
		try {
			/*
			java.util.List<String> titles = new ArrayList<String>();
			titles1.add("SDE in Amazon");
			titles1.add("MS from UTD, B.Tech from IITR");
			
			UserProfile userProfile1 = new UserProfile("varun", "sham", "kohade", "varunkohade.iitr@gmail1.com", "pass123", 
					true, false, false, Gender.MALE, "sector 12", "Delhi", IndianState.DELHI, Country.INDIA,
					110022, new PhoneNumber(989, 994, 0689), 0, 0, 0, titles, Frequency.WEEKLY, 
					new DateOfBirth(3, 5, 1987), 0, "", "", "", "");
			
					UserProfile.newBuilder()
					.setEmailId("varunkohade.iitr@gmail1.com")
					.setPassword("pass123")
					.setFirstName("varun")
					.setMiddleName("sham")
					.setLastName("kohade")
					.setGender(sex.Male)
					.setDateOfBirth("25/04/1987")
					.setReviews(0)
					.setCity("Delhi")
					.setContactNumber("+12148154585")
					.setCountry("India")
					.setState("Delhi")
					.setStreet("sector 12")
					.setZip("110022")
					.setAlmaMaters(0)
					.setFollows(0)
					.setLikes(0)
					.setTitles(titles1)
					.setValidUser(validity.True)
					.setFacebookUser(validFBUser.False)
					.setGoogleUser(validGoogleUser.False)
					.setSubscriptionFrequency(frequency.Weekly)
					.setAccountStatusChangeDate("25/04/1987")
					.setPasswordResetToken("")
					.setPasswordResetTokenCreationTime("")
					.setRegistrationToken("")
					.build();
			
			UserProfile userProfile2 = UserProfile.newBuilder()
					.setEmailId("nitish.kohade@gmail.com")
					.setPassword("nit123")
					.setFirstName("nitish")
					.setMiddleName("sham")
					.setLastName("kohade")
					.setGender(sex.Male)
					.setDateOfBirth("24/12/1989")
					.setReviews(0)
					.setCity("Delhi")
					.setContactNumber("+12148154585")
					.setCountry("India")
					.setState("Delhi")
					.setStreet("sector 12")
					.setZip("110022")
					.setAlmaMaters(0)
					.setFollows(0)
					.setLikes(0)
					.setTitles(titles1)
					.setValidUser(validity.True)
					.setFacebookUser(validFBUser.False)
					.setGoogleUser(validGoogleUser.False)
					.setSubscriptionFrequency(frequency.Weekly)
					.setAccountStatusChangeDate("25/04/1987")
					.setPasswordResetToken("")
					.setPasswordResetTokenCreationTime("")
					.setRegistrationToken("")
					.build();
			*/	
			UserProfileClient dbClient = new UserProfileClient();
			
			//dbClient.createTable();
			
			//dbClient.putItems(userProfile1);
			//dbClient.putItems(userProfile2);
			//System.out.println(dbClient.deactivateUserProfile("nitish.kohade@gmail.com"));
			//System.out.println(dbClient.getUserProfile("varunkohade.iitr@gmail.com"));
			//System.out.println(dbClient.updateSubscriptionFrequency("nitish.kohade@gmail.com", "monthly"));
			//dbClient.deleteItem("varunkohade.iitr@gmail.com", "pass12");
		    //System.out.println(dbClient.getUserProfile("nitish.kohade@gmail.com"));
			//System.out.println(dbClient.IsValidUserProfile("nitish.kohade@gmail.com", "abc123"));
			System.out.println(dbClient.createUserProfile("nitish.kohade@gmail.com", "abc123", true, 
					false, false));
			//System.out.println(dbClient.updateUserPassword("nitish.kohade@gmail.com", "updatedPassword2", 
			//		"updatedPassword1"));
			//System.out.println(dbClient.createRegistrationToken("nitish.kohade@gmail.com"));
			//System.out.println(dbClient.IsValidUserProfile("nitish.kohade@gmail.com"));
			//System.out.println(dbClient.validateUserProfile("nitish.kohade@gmail.com", 
			//		"4ee807a3-6b7f-49e9-b0c9-df8fe12470ef"));
		    //System.out.println(dbClient.IsMatchingPasswordResetToken("varunkohade.iitr@gmail.com", 
		    //		"364da23e-a206-4596-b743-8f5b48be4ffc"));
			//System.out.println(dbClient.IsMatchingRegistrationToken("varunkohade.iitr@gmail.com", 
			//	    		"831ab024-2e3a-4e66-a235-f2eeed66695a"));
			PhoneNumber contactNumber = new PhoneNumber("989", "994", "0689");
			DateOfBirth dateOfBirth = new DateOfBirth(25, 4, 1987);
			List<String> titles = Collections.singletonList("Software Engineer");
			EditableUserProfile editableUserProfile = new  EditableUserProfile("nitish.kohade@gmail.com", "NITISH", "SHAM",
					"KOHADE", "nitish.kohade@gmail.com", Gender.MALE.getValue(), "sector 12", "Delhi", 
					IndianState.DELHI.getStateName(), "INDIA", "110022", contactNumber.toString(), titles, 
					Frequency.WEEKLY.getValue(), dateOfBirth.toString());
			//System.out.println(dbClient.updateUserProfileInfo(editableUserProfile));
			//System.out.println(dbClient.isEmailIdAvailable("varunkohade.iitr@gmail.com"));
			//System.out.println(dbClient.changeUserPassword("nitish.kohade@gmail.com", 
			//		"b17dada9-9cd5-4568-9bc1-ac35f93948df", "updatedPassword1"));
			//System.out.println(dbClient.createPasswordResetToken("nitish.kohade@gmail.com"));
			//System.out.println(dbClient.IsValidRegisteredUserProfile("nitish.kohade@gmail.com"));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}