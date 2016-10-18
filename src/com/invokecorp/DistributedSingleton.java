// Copyright Invoke Corporation. All rights reserved.
package com.invokecorp;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;

/**
 * Problem: There are N instances running the same code in an auto-scale group and only one daemon should be active.
 * Solution: Use this class to acquire a lock so that only one daemon is active. However, the other daemons are on
 * standby waiting to become active if the active daemon terminates. The pattern is to try to acquire a lock, then
 * within the running loop of the daemon to periodically try to acquire a lock, while releasing any stale locks due
 * to instances being terminated.
 * @author Matson Wade
 */
public class DistributedSingleton
{
	static final String SEMAPHORE = "semaphore";
	static final String INSTANCE_ID = "instanceID";

	private final String instanceID;
	private final String semaphoreDomain;
	private final AmazonEC2Client ec2;
	private final AmazonSimpleDBClient simpleDB;
	
	public DistributedSingleton(AmazonEC2Client ec2, AmazonSimpleDBClient simpleDB, String semaphoreDomain, String instanceID)
	{
		this.ec2 = ec2;
		this.simpleDB = simpleDB;
		this.semaphoreDomain = semaphoreDomain;
		this.instanceID = instanceID;
	}
	
	public boolean acquireLock(String daemonName, Logger logger)
	{
		boolean status = false;
		
		try
			{
			List<Attribute> attributes = SDBUtils.getConsistentAttributes(simpleDB, semaphoreDomain, daemonName, logger);
			
			String semaphore = SDBUtils.getAttributeValue(attributes, SEMAPHORE);
			
			String instanceID = SDBUtils.getAttributeValue(attributes, INSTANCE_ID);
			
			if (semaphore.equals("0"))
				{
				SDBUtils.putConditionalAttribute(simpleDB, semaphoreDomain, daemonName, SEMAPHORE, "0" /*old value*/, "1" /*new value*/, logger);
				
				SDBUtils.putAttribute(simpleDB, semaphoreDomain, daemonName, INSTANCE_ID, this.instanceID, true, logger);
				
				logger.warn("Aquired lock with instance ID " + this.instanceID);
				
				status = true;
				}
			else
				logger.warn("Unable to aquired lock because instance " + instanceID + " is busy running");
			}
		catch (Exception e)
			{
			logger.warn("Failed to acquire lock!", e);
			}
		
		return status;
	}
	
	public void releaseLock(String daemonName, long waitTime, Logger logger)
	{
		// Make several attempts to release the lock.
		for (int i = 0; i < 3; i++)
			try
				{
				doReleaseLock(daemonName, logger);
				break;
				}
			catch (Exception e1)
				{
				logger.error("Failed to release lock!", e1);
				
				if (waitTime > 0)
					try
						{
						logger.warn("Sleeping for " + waitTime + " msecs");
						Thread.sleep(waitTime);
						}
					catch (Exception e2)
						{
						logger.error("Sleep failed", e2);
						}
				}
	}
	
	private void doReleaseLock(String daemonName, Logger logger) throws Exception
	{
		SDBUtils.putConditionalAttribute(simpleDB, semaphoreDomain, daemonName, SEMAPHORE, "1" /*old value*/, "0" /*new value*/, logger);
		
		logger.warn("Released lock to allow another instance to run");
		
		SDBUtils.putAttribute(simpleDB, semaphoreDomain, daemonName, INSTANCE_ID, "", true, logger);
		
		logger.warn("Disassociated lock from any instance ID");
	}
	
	/**
	 * This method clears any stale lock and MUST be called during the initialization phase for every daemon that's a distributed singleton.
	 */
	public void releaseAnyStaleLock(String daemonName, Logger logger) throws Exception
	{
		List<Attribute> attributes = SDBUtils.getConsistentAttributes(simpleDB, semaphoreDomain, daemonName, logger);
	
		if (attributes == null || attributes.isEmpty())
			{
			SDBUtils.putAttribute(simpleDB, semaphoreDomain, daemonName, SEMAPHORE, "0", true, logger);
			logger.warn("Created semaphore for " + daemonName + " in " + semaphoreDomain);
			}
		else
			{
			String instanceID = SDBUtils.getAttributeValue(attributes, INSTANCE_ID);
			
			// Make sure associated instance is still running. If not, then clear the lock.
			if (instanceID == null || (EC2Utils.isInstanceRunning(ec2, instanceID, logger) == false)
				|| instanceID.equals(this.instanceID))
				{
				// TODO - This just clears what's there, instead check if there's a lock and if there is call doReleaseLock.
				
				List<ReplaceableAttribute> replaceableAttributes = new ArrayList<ReplaceableAttribute>();
				
				replaceableAttributes.add(new ReplaceableAttribute(SEMAPHORE, "0", true));
				
				replaceableAttributes.add(new ReplaceableAttribute(INSTANCE_ID, "", true));
	
				SDBUtils.putAttributes(simpleDB, semaphoreDomain, daemonName, replaceableAttributes, logger);
	
				logger.warn("Released stale lock for " + daemonName + " owned by instance " + instanceID);
				}
			}	
	}
}