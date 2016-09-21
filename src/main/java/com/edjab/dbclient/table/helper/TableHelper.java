//@Copyright Edjab 2016

package com.edjab.dbclient.table.helper;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;

/**
 * @author varun
 *
 */
public class TableHelper {
	
	private static final Logger LOGGER = Logger.getLogger(TableHelper.class);
	
	//Tested
	public static String getEncryptPasswordMD5(String input) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] messageDigest = md.digest(input.getBytes());
			BigInteger number = new BigInteger(1, messageDigest);
			String hashText = number.toString(16);
			while(hashText.length() < 32) {
				hashText = '0' + hashText;
			}
			return hashText;
		}
		catch(Exception exception) {
			throw new RuntimeException(exception);
		}
	}
	
	//Tested
	public static String getTableStatus(final AmazonDynamoDBClient amazonDynamoDBClient, 
			final String tableName) {
		final TableDescription tableDescription = amazonDynamoDBClient.describeTable(
				new DescribeTableRequest().withTableName(tableName)).getTable();
		return tableDescription.getTableStatus();
	}
	
	//Tested
	public static void describeTable(final AmazonDynamoDBClient amazonDynamoDBClient, 
			final String tableName) {
		LOGGER.info("Describing table " + tableName);
		final TableDescription tableDescription = amazonDynamoDBClient.describeTable(
				new DescribeTableRequest().withTableName(tableName)).getTable();
		final String description = String.format(
				"%s: %s \t ReadCapacityUnits: %d \t WriteCapacityUnits: %d",
				tableDescription.getTableStatus(), tableDescription
						.getTableName(), tableDescription
						.getProvisionedThroughput().getReadCapacityUnits(),
				tableDescription.getProvisionedThroughput()
						.getWriteCapacityUnits());
		LOGGER.info(description);
	}

	//Tested
	public static void updateTable(final AmazonDynamoDBClient amazonDynamoDBClient, 
			final String tableName, final Long readCapacityUnits, final Long writeCapacityUnits) {
		
		LOGGER.info("Updating table " + tableName);
		final ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
				.withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(writeCapacityUnits);

		final UpdateTableRequest updateTableRequest = new UpdateTableRequest()
				.withTableName(tableName).withProvisionedThroughput(
						provisionedThroughput);

		final UpdateTableResult result = amazonDynamoDBClient.updateTable(updateTableRequest);
		LOGGER.info("Updated table " + result.getTableDescription().getTableName());
		
	}
	
	//Tested
	public static void listTables(final AmazonDynamoDBClient amazonDynamoDBClient) {
		
		LOGGER.info("Listing tables");
		// Initial value for the first page of table names.
		String lastEvaluatedTABLE_NAME = null;
		do {

			final ListTablesRequest listTablesRequest = new ListTablesRequest()
					.withLimit(10).withExclusiveStartTableName(
							lastEvaluatedTABLE_NAME);

			final ListTablesResult result = amazonDynamoDBClient.listTables(listTablesRequest);
			lastEvaluatedTABLE_NAME = result.getLastEvaluatedTableName();

			for (final String name : result.getTableNames()) {
				LOGGER.info(name);
			}

		} while (lastEvaluatedTABLE_NAME != null);
	}
	
	//Tested
	public static void deleteTable(final AmazonDynamoDBClient amazonDynamoDBClient, 
			final String tableName) {
		
		LOGGER.info("Deleting table " + tableName);
		final DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
				.withTableName(tableName);
		final DeleteTableResult result = amazonDynamoDBClient.deleteTable(deleteTableRequest);
		LOGGER.info("Deleted table " + result.getTableDescription().getTableName());
	}
	
	//Tested
	public static void listItems(final AmazonDynamoDBClient amazonDynamoDBClient, 
			final String tableName) {
		
		LOGGER.info("List all items");
		final ScanRequest scanRequest = new ScanRequest().withTableName(tableName);

		final ScanResult result = amazonDynamoDBClient.scan(scanRequest);
		for (final Map<String, AttributeValue> item : result.getItems()) {
			TableHelper.printItem(item);
		}
	}
	
	//Tested
	public static void printItem(final Map<String, AttributeValue> attributeList) {
		String itemString = new String();
		for (final Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
			if (!itemString.equals(""))
				itemString += ", ";
			final String attributeName = item.getKey();
			final AttributeValue value = item.getValue();
			itemString += attributeName
					+ ""
					+ (value.getS() == null ? "" : "=\"" + value.getS() + "\"")
					+ (value.getN() == null ? "" : "=\"" + value.getN() + "\"")
					+ (value.getB() == null ? "" : "=\"" + value.getB() + "\"")
					+ (value.getSS() == null ? "" : "=\"" + value.getSS()
							+ "\"")
					+ (value.getNS() == null ? "" : "=\"" + value.getNS()
							+ "\"")
					+ (value.getBS() == null ? "" : "=\"" + value.getBS()
							+ "\" \n");
		}
		
		LOGGER.info(itemString);
	}
}