// Copyright Invoke Corporation. All rights reserved.
package com.invokecorp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.UpdateCondition;
import com.amazonaws.services.simpledb.util.SimpleDBUtils;

public class SDBUtils
{
	static final int MAX_RETRIES = 100;
	static final int INITIAL_BACK_OFF_TIME = 500;
	
	public static void createDomain(AmazonSimpleDBClient simpleDB, String domain, Logger logger) throws Exception
	{
		long multiplier, backOffTime = INITIAL_BACK_OFF_TIME; // Milliseconds to back-off calling BatchPutAttributes

		// SimpleDB may throw Service Unavailable (503) exception. Therefore utilize retries with linear back-off.
		for (multiplier = 1; multiplier < MAX_RETRIES; multiplier++, backOffTime = multiplier * INITIAL_BACK_OFF_TIME)
			try
				{
				long startTime = System.currentTimeMillis();
				
				simpleDB.createDomain(new CreateDomainRequest(domain));
				
				logger.info("SimpleDB create of " + domain + " domain took " + (System.currentTimeMillis() - startTime) + " msecs");
				
				break;
				}
			catch (Exception e1)
				{
				if (e1 instanceof AmazonServiceException && ((AmazonServiceException)e1).getStatusCode() == 503)
					{
					try
						{
						logger.info("SimpleDB is unavailable so backing off for " + backOffTime + " msecs...");
						Thread.sleep(backOffTime);
						}
					catch (Exception e2) { logger.error("Sleep failed", e2); }
					}
				else
					throw new Exception("Something went wrong creating " + domain + " domain; " + e1.getMessage());
				}
		
		if (multiplier >= MAX_RETRIES)
			throw new Exception("SimpleDB service remains unavailable after " + MAX_RETRIES + " retries!");
	}
	
	public static List<ReplaceableAttribute> convertToReplaceable(List<Attribute> attributes)
	{
		List<ReplaceableAttribute> replaceableAttributes = new ArrayList<ReplaceableAttribute>();
		
		for (Iterator<Attribute> iterator = attributes.iterator(); iterator.hasNext();)
			{
			Attribute attribute = iterator.next();
			
			ReplaceableAttribute replaceableAttribute = new ReplaceableAttribute(attribute.getName(), attribute.getValue(), false);
			
			replaceableAttributes.add(replaceableAttribute);
			}
		
		return replaceableAttributes;
	}
	
	public static String getAttributeValue(List<Attribute> attributes, String attributeName)
	{
		for (Iterator<Attribute> iterator = attributes.iterator(); iterator.hasNext(); )
			{
			Attribute attribute = iterator.next();
	
			if (attribute.getName().equals(attributeName))
				return attribute.getValue();				
			}
		
		return null;
	}

	public static String[] getAttributeValues(List<Attribute> attributes, String attributeName)
	{
		ArrayList<String> multipleValues = new ArrayList<String>();
		
		for (Iterator<Attribute> iterator = attributes.iterator(); iterator.hasNext(); )
			{
			Attribute attribute = iterator.next();
	
			if (attribute.getName().equals(attributeName))
				multipleValues.add(attribute.getValue());				
			}
		
		return multipleValues.toArray(new String[] {});
	}
		
	public static boolean containsValue(List<Attribute> attributes, String attributeName, String attributeValue)
	{
		for (Iterator<Attribute> iterator = attributes.iterator(); iterator.hasNext(); )
			{
			Attribute attribute = iterator.next();
	
			String name = attribute.getName(), value = attribute.getValue();
			
			if (name.equals(attributeName) && value.equals(attributeValue))
				return true;				
			}
		
		return false;
	}

	public static void updateAttribute(AmazonSimpleDBClient simpleDB, String domain, String itemName,
			String attribute, String value, boolean replace, Logger logger) throws Exception
	{	
		List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
		
		attributes.add(new ReplaceableAttribute(attribute, value, replace));
				
		long sdbResponseTime = System.currentTimeMillis();
		
		simpleDB.putAttributes(new PutAttributesRequest(domain, itemName, attributes));
		
		logger.info("SimpleDB update of item [" + itemName + "] in " + domain + " domain took " + (System.currentTimeMillis() - sdbResponseTime) + " msecs");
	}

	public static void updateMultiValueAttribute(AmazonSimpleDBClient simpleDB, String domain, String itemName,
			String attribute, String[] values, Logger logger) throws Exception
	{	
		List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
		
		for (int i = 0; i < values.length; i++)
			attributes.add(new ReplaceableAttribute(attribute, values[i], false));
				
		long sdbResponseTime = System.currentTimeMillis();
		
		simpleDB.putAttributes(new PutAttributesRequest(domain, itemName, attributes));
		
		logger.info("SimpleDB update of item [" + itemName + "] in " + domain + " domain took " + (System.currentTimeMillis() - sdbResponseTime) + " msecs");
	}
	
	public static void deleteAttribute(AmazonSimpleDBClient simpleDB, String domain, String itemName,
			String attribute, String value, Logger logger) throws Exception
	{
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		attributes.add(new Attribute(attribute, value));
				
		long sdbResponseTime = System.currentTimeMillis();
		
		simpleDB.deleteAttributes(new DeleteAttributesRequest(domain, itemName, attributes));
		
		logger.info("SimpleDB delete of attribute [" + attribute + "] with value [" + value + "] for item [" + itemName + "] in " + domain + " domain took " + (System.currentTimeMillis() - sdbResponseTime) + " msecs");
	}
	
	public static void deleteAttributes(AmazonSimpleDBClient simpleDB, String domain, String itemName,
			List<Attribute> attributes, Logger logger) throws Exception
	{
		long sdbResponseTime = System.currentTimeMillis();
		
		simpleDB.deleteAttributes(new DeleteAttributesRequest(domain, itemName, attributes));
		
		logger.info("SimpleDB delete of [" + itemName + "] from " + domain + " domain took " + (System.currentTimeMillis() - sdbResponseTime) + " msecs");
	}
	
	public static void deleteItem(AmazonSimpleDBClient simpleDB, String domain, Item sdbItem, Logger logger) throws Exception
	{
		long sdbResponseTime = System.currentTimeMillis();
		
		List<Attribute> attributes = sdbItem.getAttributes();
		
		simpleDB.deleteAttributes(new DeleteAttributesRequest(domain, sdbItem.getName(), attributes));
		
		logger.info("SimpleDB delete of [" + sdbItem.getName() + "] from " + domain + " domain took " + (System.currentTimeMillis() - sdbResponseTime) + " msecs");
	}
	
	public static void deleteItems(AmazonSimpleDBClient simpleDB, String domain, List<Item> sdbItems, Logger logger) throws Exception
	{	
		long sdbResponseTime = System.currentTimeMillis();
		
		for (int i = 0; i < sdbItems.size(); i++)
			deleteItem(simpleDB, domain, sdbItems.get(i), logger);
		
		logger.info("SimpleDB delete of [" + sdbItems.size() + "] items(s) from " + domain + " domain took " + (System.currentTimeMillis() - sdbResponseTime) + " msecs");
	}
	
	public static long updateCounter(AmazonSimpleDBClient simpleDB, String domain, String itemName,
		String attributeName, long delta, int maxNumDigits, Logger logger) throws Exception
	{
		long newValue = 0;
		
		long multiplier, backOffTime = INITIAL_BACK_OFF_TIME; // Milliseconds to back-off calling BatchPutAttributes

		// The conditional put might failed. Therefore utilize retries with linear back-off.
		for (multiplier = 1; multiplier < MAX_RETRIES; multiplier++, backOffTime = multiplier * INITIAL_BACK_OFF_TIME)
			try
				{
				List<Attribute> attributes = getConsistentAttributes(simpleDB, domain, itemName, logger); // CHANGED TO CONSISTENT READ
							
				String oldValue = getAttributeValue(attributes, attributeName);
										
				newValue = 0;
				
				if (oldValue == null || oldValue.isEmpty())
					{
					logger.info("Creating attribute " + attributeName + " with value " + delta + " since it doesn't exist for " + itemName);
					
					if (delta > 0)
						{
						newValue = delta;
						
						String zeroPaddedNumber = SimpleDBUtils.encodeZeroPadding(newValue, maxNumDigits);
						
						putAttribute(simpleDB, domain, itemName, attributeName, zeroPaddedNumber, true, logger);
						}
					}
				else
					{
					newValue = Long.parseLong(oldValue) + delta;

					String zeroPaddedNumber = SimpleDBUtils.encodeZeroPadding(newValue, maxNumDigits);
					
					putConditionalAttribute(simpleDB, domain, itemName, attributeName, oldValue, zeroPaddedNumber, logger);
					}
				
				break;
				}
			catch (Exception e1)
				{
				if (e1 instanceof AmazonServiceException && ((AmazonServiceException)e1).getStatusCode() == 409)
					{
					try
						{
						logger.info("Conditional put failed so backing off for " + backOffTime + " msecs...");
						Thread.sleep(backOffTime);
						}
					catch (Exception e2) { logger.error("Sleep failed", e2); }
					}
				else
					throw new Exception("Something went wrong updating counter; " + e1.getMessage());
				}
		
		if (multiplier >= MAX_RETRIES)
			throw new Exception("Conditional put continues to fail after " + MAX_RETRIES + " retries!");
		
		return newValue;
	}
	
	public static List<Attribute> getAttributes(AmazonSimpleDBClient simpleDB, String domain, String itemName, Logger logger) throws Exception
	{
		long sdbResponseTime = System.currentTimeMillis();
		
		GetAttributesResult sdbGetResult = simpleDB.getAttributes(new GetAttributesRequest(domain, itemName));
		
		logger.info("SimpleDB get item [" + itemName + "] from " + domain + " domain took " + (System.currentTimeMillis() - sdbResponseTime) + " msecs");
	
		return sdbGetResult.getAttributes();
	}
	
	public static List<Attribute> getConsistentAttributes(AmazonSimpleDBClient simpleDB, String domain, String itemName, Logger logger) throws Exception
	{
		long sdbResponseTime = System.currentTimeMillis();
		
		GetAttributesRequest gar = new GetAttributesRequest(domain, itemName);
		
		gar.setConsistentRead(true);
		
		GetAttributesResult sdbGetResult = simpleDB.getAttributes(gar);

		logger.info("SimpleDB consistent get item [" + itemName + "] from " + domain + " domain took " + (System.currentTimeMillis() - sdbResponseTime) + " msecs");
	
		return sdbGetResult.getAttributes();
	}
		
	public static void putAttributes(AmazonSimpleDBClient simpleDB, String domain, String itemName, List<ReplaceableAttribute> attributes, Logger logger)
	{
		if (simpleDB == null)
			return;
		
		long sdbResponseTime = System.currentTimeMillis();
		
		simpleDB.putAttributes(new PutAttributesRequest(domain, itemName, attributes));
		
		logger.info("SimpleDB put item [" + itemName + "] in " + domain + " domain took " + (System.currentTimeMillis() - sdbResponseTime) + " msecs");
	}
	
	public static void putAttribute(AmazonSimpleDBClient simpleDB, String domain, String itemName, String attributeName, String attributeValue, boolean replace, Logger logger)
	{
		List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
		
		attributes.add(new ReplaceableAttribute(attributeName, attributeValue, replace));
		
		putAttributes(simpleDB, domain, itemName, attributes, logger);
	}
	
	public static void putConditionalAttribute(AmazonSimpleDBClient simpleDB, String domain, String itemName, String attributeName, String oldAttributeValue, String newAttributeValue, Logger logger)
	{
		long sdbResponseTime = System.currentTimeMillis();
	
		List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
		
		attributes.add(new ReplaceableAttribute(attributeName, newAttributeValue, true));
		
		// Do conditional put to update the attribute.
		simpleDB.putAttributes(new PutAttributesRequest(domain, itemName, attributes,
			new UpdateCondition(attributeName, oldAttributeValue, true)));
		
		logger.info("SimpleDB conditional put item [" + itemName + "] in " + domain + " domain took " + (System.currentTimeMillis() - sdbResponseTime) + " msecs");
	}
}