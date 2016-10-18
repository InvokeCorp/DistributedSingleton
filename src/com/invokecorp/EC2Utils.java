// Copyright Invoke Corporation. All rights reserved.
package com.invokecorp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.InstanceStatus;

public class EC2Utils
{
	static final String GET_INSTANCE_ID = "http://169.254.169.254/latest/meta-data/instance-id";

	public static String getInstanceID() throws Exception
	{
		return RestClient.remoteOperation(GET_INSTANCE_ID, RestClient.IS_HTTP_GET, new HashMap<String, String>(), null);
	}
	
	public static boolean isInstanceRunning(AmazonEC2Client ec2Client, String instanceID, Logger logger)
	{
		if (instanceID == null || instanceID.trim().length() == 0)
			return false;
		
		try
			{
			DescribeInstanceStatusRequest statusRequest = new DescribeInstanceStatusRequest();
			
			ArrayList<String> instanceIDs = new ArrayList<String>();
			
			instanceIDs.add(instanceID);
			
			statusRequest.setInstanceIds(instanceIDs);
			
			DescribeInstanceStatusResult statusResult = ec2Client.describeInstanceStatus(statusRequest);
			
			List<InstanceStatus> status = statusResult.getInstanceStatuses();
			
			if (status.size() > 0 && status.get(0).getInstanceStatus().getStatus().equals("ok")
				&& status.get(0).getInstanceState().getName().equals("running"))
				return true;
			
			logger.warn("Instance " + instanceID + " isn't running!");
			}
		catch (Exception e)
			{
			logger.warn(e.getMessage());
			}
		
		return false;
	}
}