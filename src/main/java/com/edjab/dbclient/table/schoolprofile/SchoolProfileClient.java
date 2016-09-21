//@Copyright Edjab 2016

package com.edjab.dbclient.table.schoolprofile;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.edjab.dbclient.table.helper.TableHelper;
import com.edjab.model.Country;
import com.edjab.model.Direction;
import com.edjab.model.IndianState;
import com.edjab.model.InstituteProfile;
import com.edjab.model.PhoneNumber;
import com.edjab.model.Region;
import com.edjab.model.SchoolCategory;
import com.edjab.model.VisibleInstituteProfile;

/**
 * This is the School Profile Client Class whose act as the application class for all the retrieval, 
 * updates w.r.t NoSqlDB.
 * @author varun
 *
 */
public class SchoolProfileClient {
	
	private static final Logger LOGGER = Logger.getLogger(SchoolProfileClient.class);
	private static final String TABLE_NAME = "SchoolProfileEdjabProd";
	private final AmazonDynamoDBClient amazonDynamoDBClient;
	
	/**
	 * Default Constructor
	 * 1) Do some testing by adding client configuration and setting maxRetry method also see max connection method.
	 * It is not thread safe. this applies to all classes.
	 * @throws IOException
	 */
	public SchoolProfileClient() throws IOException {
		BasicConfigurator.configure();
		AWSCredentials credentials;
		try {
		       credentials= new PropertiesCredentials(
		    		   SchoolProfileClient.class
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
	public SchoolProfileClient(final AmazonDynamoDBClient dynamoDBClient) throws IOException {
		checkNotNull(dynamoDBClient, "dynamoDBClient must not be null");
		BasicConfigurator.configure();
		amazonDynamoDBClient = dynamoDBClient;
	}
	
	/**
	 * TODO: Tested. Change partition
	 * @throws Exception
	 */
	public void createTable() throws Exception {
		try {
			
		LOGGER.info("Creating table " + TABLE_NAME);
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"country").withAttributeType("S"));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"schoolnameid").withAttributeType("S"));


		ArrayList<KeySchemaElement> tableKeySchemaElement = new ArrayList<KeySchemaElement>();
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("country").withKeyType(
				KeyType.HASH));
		tableKeySchemaElement.add(new KeySchemaElement().withAttributeName("schoolnameid").withKeyType(
				KeyType.RANGE));
		
		final ProvisionedThroughput tableProvisionedThroughput = new ProvisionedThroughput()
		.withReadCapacityUnits(2L).withWriteCapacityUnits(1L);
		
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
	 * TODO: Tested. Rounding off should be taken care in case of average rating 
	 * @param schoolid
	 * @return
	 */
	public float getAverageRating(final String schoolid) {
		checkArgument(StringUtils.isNotBlank(schoolid), "Schoolid must not be blank");
		int averageRating = 0;
		int totalRating = 0;
		try {
			Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("country", new AttributeValue().withS("INDIA"));
			key.put("schoolnameid", new AttributeValue().withS(schoolid));
			final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
			final Map<String, AttributeValue> items = getItemResult.getItem();
			
			for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
				String attributeName = item.getKey();
				String value = "";
				if(attributeName.equals("threeStarRatings")) {
					value = item.getValue().getN();
					if(value != null) {
						averageRating = averageRating + (3*Integer.parseInt(value));
						totalRating = totalRating + Integer.parseInt(value);
						System.out.println(attributeName + "      " + value + "   " + averageRating);
					}
				}
				if(attributeName.equals("fiveStarRatings")) {
					value = item.getValue().getN();
					if(value != null) {
						averageRating = averageRating + (5*Integer.parseInt(value));
						totalRating = totalRating + Integer.parseInt(value);
						System.out.println(attributeName + "      " + value + "   " + averageRating);
					}
				}
				if(attributeName.equals("fourStarRatings")) {
					value = item.getValue().getN();
					if(value != null) {
						averageRating = averageRating + (4*Integer.parseInt(value));
						totalRating = totalRating + Integer.parseInt(value);
						System.out.println(attributeName + "      " + value + "   " + averageRating);
					}
				}
			    if(attributeName.equals("twoStarRatings")) {
					value = item.getValue().getN();
					if(value != null) {
						averageRating = averageRating + (2*Integer.parseInt(value));
						totalRating = totalRating + Integer.parseInt(value);
						System.out.println(attributeName + "      " + value + "   " + averageRating);
					}
				}
				if(attributeName.equals("oneStarRatings")) {
					value = item.getValue().getN();
					if(value != null) {
						averageRating = averageRating + Integer.parseInt(value);
						totalRating = totalRating + Integer.parseInt(value);
						System.out.println(attributeName + "      " + value + "   " + averageRating);
					}
				}
			}
			if(averageRating != 0.0f) {
			return (float)(averageRating/(float)totalRating);
			}
			else {
				return 0;
			}
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : getAverageRating, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : getAverageRating, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : getAverageRating, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : getAverageRating, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return averageRating;
	}
	
	/**
	 * Tested. putting all necessary items in the table
	 * @param instituteProfile
	 * @return
	 * @throws InterruptedException
	 */
	public boolean putSchoolItems(final InstituteProfile instituteProfile) throws InterruptedException {
		
		checkNotNull(instituteProfile, "InstituteProfile must not be null");
		
		LOGGER.info("Putting items into table " + TABLE_NAME);
		try {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		
		item.put("schoolnameid", new AttributeValue().withS(instituteProfile.getInstituteId().toUpperCase()));
		item.put("name", new AttributeValue().withS(instituteProfile.getInstituteName().toUpperCase()));
		
		java.util.List<String> urls = instituteProfile.getUrls();
		if(urls != null) {
		    item.put("urls", new AttributeValue().withSS(urls));
		}
		else {
			item.put("urls", new AttributeValue().withSS(" "));
		}
		
		String emailid = instituteProfile.getEmailId();
		if(emailid != null) {
			item.put("emailId", new AttributeValue().withS(emailid.toLowerCase()));
		}
		else {
			item.put("emailId", new AttributeValue().withS(" "));
		}
		
		String street = instituteProfile.getStreet();
		if(street != null) {
			item.put("street", new AttributeValue().withS(street.toLowerCase()));
		}
		else {
			item.put("street", new AttributeValue().withS(" "));
		}
		
		item.put("city", new AttributeValue().withS(instituteProfile.getCity().toLowerCase()));
		item.put("stateOrRegion", new AttributeValue().withS(instituteProfile.getIndianState().toString().toUpperCase()));
		item.put("country", new AttributeValue().withS(instituteProfile.getCountry().toString().toUpperCase()));
		item.put("region", new AttributeValue().withS(instituteProfile.getRegion().toString().toUpperCase()));
		item.put("direction", new AttributeValue().withS(instituteProfile.getDirection().toString().toUpperCase()));
		item.put("zip", new AttributeValue().withS(instituteProfile.getZip()));
		item.put("latitude", new AttributeValue().withS(instituteProfile.getLatitude().toString()));
		item.put("longitude", new AttributeValue().withS(instituteProfile.getLongitude().toString()));
		item.put("averageRating", new AttributeValue().withS(instituteProfile.getAverageRating().toString()));
		item.put("initialAverageRating", new AttributeValue().withS(instituteProfile.getInitialAverageRating().toString()));
		
		String mission = instituteProfile.getMission();
		if(mission != null) {
			item.put("mission", new AttributeValue().withS(mission.toLowerCase()));
		}
		else {
			item.put("mission", new AttributeValue().withS(" "));
		}
		
		PhoneNumber contactNumber = instituteProfile.getContactNumber();
		if(contactNumber != null) {
			item.put("contactNumber", new AttributeValue().withS(contactNumber.toString()));
		}
		else {
			item.put("contactNumber", new AttributeValue().withS(" "));
		}
		
		String description = instituteProfile.getDescription();
		if(description != null) {
			item.put("description", new AttributeValue().withS(description));
		}
		else {
			item.put("description", new AttributeValue().withS(" "));
		}
		
		String profileImage = instituteProfile.getProfileImageUrl();
		if(profileImage != null) {
			item.put("profileImage", new AttributeValue().withS(profileImage));
		}
		else {
			item.put("profileImage", new AttributeValue().withS(" "));
		}
		
		java.util.List<SchoolCategory> categoryList = instituteProfile.getCategories();
		java.util.List<String> categories = new ArrayList<String>(categoryList.size());
		for(SchoolCategory items :categoryList) {
			String category = items.toString().toUpperCase();
			categories.add(category);
		}
		item.put("categories", new AttributeValue().withSS(categories));
		
		item.put("attendees", new AttributeValue().withN(instituteProfile.getAttendees().toString()));
		item.put("followers", new AttributeValue().withN(instituteProfile.getFollowers().toString()));
		item.put("likes", new AttributeValue().withN(instituteProfile.getLikes().toString()));
		
		String establishmentDate = instituteProfile.getDateOfEstablishment();
		if(establishmentDate != null) {
			item.put("establishmentDate", new AttributeValue().withS(establishmentDate));
		}
		else {
			item.put("establishmentDate", new AttributeValue().withS(" "));
		}
		
		item.put("reviews", new AttributeValue().withN(instituteProfile.getReviews().toString()));
		item.put("oneStarRatings", new AttributeValue().withN(instituteProfile.getOneStarRatings().toString()));
		item.put("twoStarRatings", new AttributeValue().withN(instituteProfile.getTwoStarRatings().toString()));
		item.put("threeStarRatings", new AttributeValue().withN(instituteProfile.getThreeStarRatings().toString()));
		item.put("fourStarRatings", new AttributeValue().withN(instituteProfile.getFourStarRatings().toString()));
		item.put("fiveStarRatings", new AttributeValue().withN(instituteProfile.getFiveStarRatings().toString()));
		
		PutItemRequest itemRequest = new PutItemRequest().withTableName(
				TABLE_NAME).withItem(item);
		amazonDynamoDBClient.putItem(itemRequest);
		return true;
		}
		catch(ConditionalCheckFailedException conditionalCheckFailedException) {
			LOGGER.error("API : putSchoolItems, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : putSchoolItems, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : putSchoolItems, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : putSchoolItems, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : putSchoolItems, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	/**
	 * Tested. Is schoolProfile valid
	 * @param schoolnameid
	 * @return
	 */
	public boolean isValidSchoolProfile(final String schoolnameid) {
		checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("country", new AttributeValue().withS("INDIA"));
		key.put("schoolnameid", new AttributeValue().withS(schoolnameid));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		if(items.entrySet() != null) {
			return true;
		}
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : isValidSchoolProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : isValidSchoolProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : isValidSchoolProfile, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : isValidSchoolProfile, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return false;
	}
	
	/**
	 * Tested. Gets School Profile
	 * @param schoolnameid
	 * @return
	 */
	public VisibleInstituteProfile getSchoolProfile(final String schoolnameid) {
		checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid must not be blank");
		try {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("country", new AttributeValue().withS("INDIA"));
		key.put("schoolnameid", new AttributeValue().withS(schoolnameid));
		final GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, key);
		final Map<String, AttributeValue> items = getItemResult.getItem();
		
		String instituteName =  "";
		List<String> urls = null;
		String emailId = "";
		String street = "";
		String city = "";
		String indianState = "";
		String country = "";
		String zip = "";
		Double latitude = 0.0;
		Double longitude = 0.0;
		String mission = "";
		String contactNumber = "";
		String description = "";
		String profileImageUrl = "";
		List<String> categories = null;
		Integer reviews = 0;
		Integer likes = 0;
		Integer attendees = 0;
		Integer follows = 0;
		String dateOfEstablishment = "";
		Integer oneStarRatings = 0;
		Integer twoStarRatings = 0;
		Integer threeStarRatings = 0;
		Integer fourStarRatings = 0;
		Integer fiveStarRatings = 0;
		Double averageRating = 0.0;
		Double initialAverageRating = 0.0;
		
		for (final Map.Entry<String, AttributeValue> item : items.entrySet()) {
				String attributeName = item.getKey();
				String value = "";
				
				if(attributeName.equals("stateOrRegion")) {
					value = item.getValue().getS();
					if(value != null) {
						indianState =  value;
					}
				}
				if(attributeName.equals("establishmentDate")) {
					value = item.getValue().getS();
					if(value != null) {
						dateOfEstablishment = value;
					}
				}
				if(attributeName.equals("city")) {
					value = item.getValue().getS();
					if(value != null) {
						city =  value;
					}
				}
				if(attributeName.equals("threeStarRatings")) {
					value = item.getValue().getN();
					if(value != null) {
						threeStarRatings =  Integer.parseInt(value);
					}
				}
				if(attributeName.equals("latitude")) {
					value = item.getValue().getS();
					if(value != null) {
						latitude =  Double.parseDouble(value);
					}
				}
				if(attributeName.equals("averageRating")) {
					value = item.getValue().getS();
					if(value != null) {
						averageRating =  Double.parseDouble(value);
					}
				}
				if(attributeName.equals("initialAverageRating")) {
					value = item.getValue().getS();
					if(value != null) {
						initialAverageRating =  Double.parseDouble(value);
					}
				}
				if(attributeName.equals("description")) {
					value = item.getValue().getS();
					if(value != null) {
						description =  value;
					}
				}
				if(attributeName.equals("emailId")) {
					value = item.getValue().getS();
					if(value != null) {
						emailId =  value;
					}
				}
				if(attributeName.equals("profileImage")) {
					value = item.getValue().getS();
					if(value != null) {
						profileImageUrl = value;
					}
				}
				if(attributeName.equals("urls")) {
					java.util.List<String> listvalue = (java.util.List<String>) item.getValue().getSS();
					if(listvalue != null) {
						urls = listvalue;
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
				if(attributeName.equals("categories")) {
					java.util.List<String> listvalue = (java.util.List<String>) item.getValue().getSS();
					if(listvalue != null) {
						categories = listvalue;
					}
				}
				if(attributeName.equals("longitude")) {
					value = item.getValue().getS();
					if(value != null) {
						longitude =  Double.parseDouble(value);
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
						reviews = Integer.parseInt(value);
					}
				}
				if(attributeName.equals("attendees")) {
					value = item.getValue().getN();
					if(value != null) {
						attendees = Integer.parseInt(value);
					}
				}
				if(attributeName.equals("followers")) {
					value = item.getValue().getN();
					if(value != null) {
						follows = Integer.parseInt(value);
					}
				}
				if(attributeName.equals("likes")) {
					value = item.getValue().getN();
					if(value != null) {
						likes = Integer.parseInt(value);
					}
				}
				if(attributeName.equals("fiveStarRatings")) {
					value = item.getValue().getN();
					if(value != null) {
						fiveStarRatings = Integer.parseInt(value);
					}
				}
				if(attributeName.equals("fourStarRatings")) {
					value = item.getValue().getN();
					if(value != null) {
						fourStarRatings = Integer.parseInt(value);
					}
				}
				if(attributeName.equals("twoStarRatings")) {
					value = item.getValue().getN();
					if(value != null) {
						twoStarRatings = Integer.parseInt(value);
					}
				}
				if(attributeName.equals("mission")) {
					value = item.getValue().getS();
					if(value != null) {
						mission =  value;
					}
				}
				if(attributeName.equals("name")) {
					value = item.getValue().getS();
					if(value != null) {
						instituteName =  value;
					}
				}
				if(attributeName.equals("oneStarRatings")) {
					value = item.getValue().getN();
					if(value != null) {
						oneStarRatings = Integer.parseInt(value);
					}
				}
		}
		
		return new VisibleInstituteProfile(schoolnameid, instituteName, urls, emailId, street, city, 
				indianState, country, zip, latitude, longitude, mission, contactNumber, description, 
				profileImageUrl, categories, attendees, likes, follows, reviews, dateOfEstablishment, 
				oneStarRatings, twoStarRatings, threeStarRatings, fourStarRatings, fiveStarRatings, 
				averageRating, initialAverageRating);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : getSchoolProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : getSchoolProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : getSchoolProfile, Internal Server error", 
					internalServerErrorException);
		}
		catch(NullPointerException nullPointerException) {
			LOGGER.error("API : getSchoolProfile, NullPointerException possibly because of bad parameter", 
				nullPointerException);
		}
		return null;
	}
	
	/**
	 * Deletes School Profile
	 * @param schoolnameid
	 * @return
	 */
	private boolean deleteSchoolProfile(final String schoolnameid) {
		checkArgument(StringUtils.isNotBlank(schoolnameid), "Schoolnameid must not be blank");
		try {
		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("country", new AttributeValue().withS("INDIA"));
		key.put("schoolnameid", new AttributeValue().withS(schoolnameid));

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
			LOGGER.error("API : deleteSchoolProfile, Error while condition checks", 
					conditionalCheckFailedException);
		}
		catch(ProvisionedThroughputExceededException provisionedThroughputExceededException) {
			LOGGER.error("API : deleteSchoolProfile, Could not meet with provisioned throughput", 
					provisionedThroughputExceededException);
		}
		catch(ResourceNotFoundException resourceNotFoundException) {
			LOGGER.error("API : deleteSchoolProfile, resource probably table not found", 
					resourceNotFoundException);
		}
		catch(ItemCollectionSizeLimitExceededException itemCollectionSizeLimitExceededException) {
			LOGGER.error("API : deleteSchoolProfile, Item collection size limit  excceeded", 
					itemCollectionSizeLimitExceededException);
		}
		catch(InternalServerErrorException internalServerErrorException) {
			LOGGER.error("API : deleteSchoolProfile, Internal Server error", 
					internalServerErrorException);
		}
		return false;
	}
	
	public static void main(String[] args) {
		try {
			
			java.util.List<SchoolCategory> categories1 = new ArrayList<SchoolCategory>();
			categories1.add(SchoolCategory.ENGINEERING);
			categories1.add(SchoolCategory.LAW);
			
			java.util.List<String> urls1 = new ArrayList<String>();
			urls1.add("http://www.iitr.ac.in/");
			
			PhoneNumber phoneNumber1 = new PhoneNumber("133", "2285", "311");
			
			InstituteProfile instituteProfile1 = new InstituteProfile("INDIAN_INSTITUTE_OF_TECHNOLOGY_ROORKEE_UTTARAKHAND", 
					"Indian Institute Of Technology Roorkee", urls1, "gate@iitr.ernet.in", "Roorkee- Haridwar Highway", 
					"Roorkee_UT", IndianState.UTTARAKHAND, Country.INDIA, "247667", Region.NORTH, Direction.NORTH, 
					29.862813, 77.897582, "Nothing can be achieved without hard work", phoneNumber1, 
					"Indian Institute of Technology Roorkee (commonly known as IIT Roorkee or IITR), formerly the University of Roorkee (1948–2001) and the Thomason College of Civil Engineering (1853–1948), is a public university located in Roorkee, Uttarakhand, India. Established in 1847 in British India by the then lieutenant governor, Sir James Thomason, it was given university status in 1949 and was converted into an Indian Institute of Technology (IIT) in 2001, thus becoming the seventh IIT to be declared.", 
					"https://upload.wikimedia.org/wikipedia/en/d/d4/Iitroorkee.jpg", categories1, 0, 0, 0, 0, "1847", 
					0, 0, 0, 0, 0, 0.0, 4.9, true);
			
			
			java.util.List<SchoolCategory> categories2 = new ArrayList<SchoolCategory>();
			categories2.add(SchoolCategory.ENGINEERING);
			categories2.add(SchoolCategory.ARCHITECTURE);
			
			java.util.List<String> urls2 = new ArrayList<String>();
			urls2.add("http://www.iitb.ac.in/");
			
            PhoneNumber phoneNumber2 = new PhoneNumber("222", "5722", "545");
			
			InstituteProfile instituteProfile2 = new InstituteProfile("INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA", 
					"Indian Institute Of Technology Bombay", urls2, "http://www.iitb.ac.in/", "Powai", 
					"Mumbai_MH", IndianState.MAHARASHTRA, Country.INDIA, "400076", Region.NORTH, Direction.WEST, 
					19.1334302, 72.9132679, "Knowledge is the Supreme Goal", phoneNumber2, 
					"The Indian Institute of Technology Bombay (abbreviated IITB or IIT Bombay) is a public engineering institution located in Powai, Mumbai, India. In QS World University Rankings® 2014 IIT Bombay was ranked as India’s top university.[1] It is the second-oldest (after Indian Institute of Technology Kharagpur) institute of the Indian Institutes of Technology system", 
					"https://upload.wikimedia.org/wikipedia/en/thumb/5/58/IIT_Bombay_Logo.svg/300px-IIT_Bombay_Logo.svg.png", 
					categories2, 0, 0, 0, 0, "1950", 0, 0, 0, 0, 0, 0.0, 4.9, true);
			
			
			java.util.List<SchoolCategory> categories3 = new ArrayList<SchoolCategory>();
			categories3.add(SchoolCategory.ENGINEERING);
			categories3.add(SchoolCategory.MEDICAL);
			
			java.util.List<String> urls3 = new ArrayList<String>();
			urls3.add("http://www.fiitjee.com/");
			
			PhoneNumber phoneNumber3 = new PhoneNumber("222", "5722", "545");
			
			InstituteProfile instituteProfile3 = new InstituteProfile("FIITJEE_MUMBAI_MAHARASHTRA", 
					"FiitJee Mumbai", urls3, "nothing", "nothing", 
					"Mumbai_MH", IndianState.MAHARASHTRA, Country.INDIA, "400076", Region.NORTH, Direction.WEST, 
					19.1134302, 72.1132679, "NOTHING", phoneNumber3, 
					"NO DESCRIPTION", 
					"nothing", 
					categories3, 0, 0, 0, 0, "2000", 0, 0, 0, 0, 0, 0.0, 4.0, true);

			java.util.List<String> urls4 = new ArrayList<String>();
			urls4.add("http://www.vnit.ac.in/");
			
			PhoneNumber phoneNumber4 = new PhoneNumber("712", "2222", "828");
			
			
			InstituteProfile instituteProfile4 = new InstituteProfile("VISVESVARAYA_NATIONAL_INSTITUTE_OF_TECHNOLOGY_NAGPUR_MAHARASHTRA", 
					"FiitJee Mumbai", urls4, "-", "South Ambazari Road", 
					"Nagpur_MH", IndianState.MAHARASHTRA, Country.INDIA, "440010", Region.NORTH, Direction.WEST, 
					21.125133, 79.0503136, "NOTHING", phoneNumber4, 
					"Visvesvaraya National Institute of Technology Nagpur (VNIT), also referred to as NIT Nagpur, formerly Regional College of Engineering, Nagpur and Visvesvaraya Regional College of Engineering (VRCE), is an engineering institute in Nagpur, Maharashtra, in central India. The institute has been ranked among the best fifteen engineering colleges in India. The institute was established in June 1960 and later named in honor of engineer, planner and statesman, Sir Mokshagundam Visvesvaraya. It is one of the National Institutes of Technology and in 2007 was conferred the status of Institute of National Importance.", 
					"https://upload.wikimedia.org/wikipedia/en/e/e7/Visvesvaraya_National_Institute_of_Technology_%28crest%29.png", 
					categories1, 0, 0, 0, 0, "1960", 0, 0, 0, 0, 0, 0.0, 4.7, true);
            /*
			java.util.List<categoryTypes> categories5 = new ArrayList<categoryTypes>();
			categories5.add(categoryTypes.ENGINEERING);
			categories5.add(categoryTypes.ARCHITECTURE);
			
			java.util.List<CharSequence> urls5 = new ArrayList<CharSequence>();
			urls5.add("www.iitm.ac.in");
			
			InstituteProfile instituteProfile5 = InstituteProfile.newBuilder()
					.setCategories(categories5)
					.setCity("Chennai_TN")
					.setReviews(0)
					.setContactNumber("4422578280")
					.setCountry("India")
					.setDateOfEstablishment("1959")
					.setDescription("The Indian Institute of Technology Madras is an autonomous public engineering and research institution located in Chennai (formerly Madras), Tamil Nadu, India. It is recognised as an Institute of National Importance by the Government of India. It Founded in 1959 with technical and financial assistance from the former government of West Germany, it was the third Indian Institute of Technology that was established by the Government of India through an Act of Parliament, to provide education and research facilities in engineering and technology.")
					.setFiveStarRatings(0)
					.setFourStarRatings(0)
					.setInstituteId("INDIAN_INSTITUTE_OF_TECHNOLOGY_MADRAS_TAMILNADU")
					.setLatitude(12.9908295f)
					.setLongitude(80.2360594f)
					.setMission("Effort Yields Success")
					.setName("Indian_Institute_Of_Technology_Madras_Tamilnadu")
					.setOneStarRatings(0)
					.setProfileImageUrl("https://upload.wikimedia.org/wikipedia/en/6/69/IIT_Madras_Logo.svg")
					.setState("Tamil Nadu")
					.setStreet("Beside Adyar Cancer Institute, Opposite to C.L.R.I, Sardar Patel Rd, Adyar")
					.setThreeStarRatings(0)
					.setTwoStarRatings(0)
					.setEmailId("-")
					.setUrls(urls5)
					.setZip("600036")
					.setAttendees(0)
					.setFollowers(0)
					.setLikes(0)
					.setAverageRating(0.0f)
					.setRegion("SOUTHERN")
					.setDirection("SOUTH")
					.setInitialAverageRating(4.7f)
					.build();
			
			InstituteProfile instituteProfile6 = InstituteProfile.newBuilder()
					.setCategories(categories5)
					.setCity("Delhi_DL")
					.setReviews(0)
					.setContactNumber("1126597135")
					.setCountry("India")
					.setDateOfEstablishment("1961")
					.setDescription("The Indian Institute of Technology Delhi (abbreviated IIT Delhi or IITD) is a public research university located in Delhi, India. It was declared to be Institute of National Importance by Government of India under Institutes of Technology Act. IIT Delhi is one of the two educational institutes in India which have been listed in Quacquarelli Symonds’(QS) list of top 200 universities globally in 2015.")
					.setFiveStarRatings(0)
					.setFourStarRatings(0)
					.setInstituteId("INDIAN_INSTITUTE_OF_TECHNOLOGY_DELHI_DELHI")
					.setLatitude(28.5455814f)
					.setLongitude(77.1928312f)
					.setMission("-")
					.setName("Indian_Institute_Of_Technology_Delhi_Delhi")
					.setOneStarRatings(0)
					.setProfileImageUrl("https://upload.wikimedia.org/wikipedia/en/6/66/IIT_Delhi_logo.gif")
					.setState("Delhi")
					.setStreet("Hauz Khas, New Delhi")
					.setThreeStarRatings(0)
					.setTwoStarRatings(0)
					.setEmailId("webmaster@admin.iitd.ac.in")
					.setUrls(urls5)
					.setZip("110016")
					.setAttendees(0)
					.setFollowers(0)
					.setLikes(0)
					.setAverageRating(0.0f)
					.setRegion("NORTHERN")
					.setDirection("NORTH")
					.setInitialAverageRating(4.6f)
					.build();
			
			InstituteProfile instituteProfile7 = InstituteProfile.newBuilder()
					.setCategories(categories5)
					.setCity("Guwahati_AS")
					.setReviews(0)
					.setContactNumber("+913612583000")
					.setCountry("India")
					.setDateOfEstablishment("1994")
					.setDescription("Indian Institute of Technology Guwahati (IIT Guwahati, IITG) is a public institution established by the Government of India, located in Guwahati, in the state of Assam in India. It is the sixth Indian Institute of Technology established in India. IIT Guwahati is officially recognised as an Institute of National Importance by the government of India.")
					.setFiveStarRatings(0)
					.setFourStarRatings(0)
					.setInstituteId("INDIAN_INSTITUTE_OF_TECHNOLOGY_GUWAHATI_ASSAM")
					.setLatitude(26.1929025f)
					.setLongitude(91.6950635f)
					.setMission("Knowledge Is Power")
					.setName("Indian_Institute_Of_Technology_Guwaati_Assam")
					.setOneStarRatings(0)
					.setProfileImageUrl("https://upload.wikimedia.org/wikipedia/en/1/12/IIT_Guwahati_Logo.svg")
					.setState("Assam")
					.setStreet("Near Doul Gobinda Road, Amingaon, North Guwahati")
					.setThreeStarRatings(0)
					.setTwoStarRatings(0)
					.setEmailId("pro@iitg.ernet.in")
					.setUrls(urls5)
					.setZip("781039")
					.setAttendees(0)
					.setFollowers(0)
					.setLikes(0)
					.setAverageRating(0.0f)
					.setRegion("NORTHERN")
					.setDirection("EAST")
					.setInitialAverageRating(4.8f)
					.build();
				
			InstituteProfile instituteProfile8 = InstituteProfile.newBuilder()
					.setCategories(categories5)
					.setCity("Kharagpur_WB")
					.setReviews(0)
					.setContactNumber("+913222255221")
					.setCountry("India")
					.setDateOfEstablishment("1951")
					.setDescription("The Indian Institute of Technology Kharagpur (IIT Kharagpur or IIT KGP) is a public engineering institution established by the government of India in 1951. It was the first of the IITs to be established, and is recognized as an Institute of National Importance by the government of India. As part of Nehru's dream for a free self-sufficient India, the institute was established to train scientists and engineers after India attained independence in 1947. It shares its organisational structure and undergraduate admission process with sister IITs. The students and alumni of IIT Kharagpur are informally referred to as KGPians. Among all IITs, IIT Kharagpur has the largest campus (2,100 acres), the most departments, and the highest student enrollment. IIT Kharagpur is known for its festivals: Spring Fest (Social and Cultural Festival) and Kshitij (Asia's largest Techno-Management Festival).")
					.setFiveStarRatings(0)
					.setFourStarRatings(0)
					.setInstituteId("INDIAN_INSTITUTE_OF_TECHNOLOGY_KHARAGPUR_WEST-BENGAL")
					.setLatitude(22.3149274f)
					.setLongitude(87.3105311f)
					.setMission("Excellence in action is coincidence.")
					.setName("Indian_Institute_Of_Technology_Kharagpur_West-Bengal")
					.setOneStarRatings(0)
					.setProfileImageUrl("https://upload.wikimedia.org/wikipedia/en/1/1c/IIT_Kharagpur_Logo.svg")
					.setState("West Bengal")
					.setStreet("-")
					.setThreeStarRatings(0)
					.setTwoStarRatings(0)
					.setEmailId("-")
					.setUrls(urls5)
					.setZip("721302")
					.setAttendees(0)
					.setFollowers(0)
					.setLikes(0)
					.setAverageRating(0.0f)
					.setRegion("NORTHERN")
					.setDirection("EAST")
					.setInitialAverageRating(4.7f)
					.build();
			*/
			//AWSCredentials credentials = new PropertiesCredentials(
			//		SchoolProfileClient.class
			//				.getResourceAsStream("/AwsCredentials.properties"));
			//AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient(credentials);
			//final SchoolProfileClient dbClient = new SchoolProfileClient(amazonDynamoDBClient);
			
			//dbClient.createTable();
			
			//System.out.println(dbClient.isValidSchoolProfile("FIITJEE _MUMBAI_MAHARASHTRA"));
		    //dbClient.putSchoolItems(instituteProfile1);
			//dbClient.putSchoolItems(instituteProfile2);
			//dbClient.putSchoolItems(instituteProfile3);
			//dbClient.putSchoolItems(instituteProfile4);
			
			//System.out.println(dbClient.getSchoolProfile("INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA"));
		    
			//System.out.println(dbClient.getAverageRating("INDIAN_INSTITUTE_OF_TECHNOLOGY_BOMBAY_MAHARASHTRA"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}