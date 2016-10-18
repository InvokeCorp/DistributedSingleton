// Copyright Invoke Corporation. All rights reserved.
package com.invokecorp;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;

import org.apache.commons.net.util.Base64;
import org.apache.log4j.Logger;

public class RestClient
{
	public static final boolean IS_HTTP_GET = true;
	public static final boolean IS_HTTP_POST = false;
	public static final String NO_PARAMETER = "";
	
	static Logger logger = Logger.getLogger(RestClient.class.getSimpleName());

	/**
	 * Call REST API and return the response
	 * 
	 * @param restAPI
	 *            REST API to be called
	 * @param isHttpGet
	 *            Set to true if this is an HTTP GET, otherwise set to false to indicate that it's an HTTP POST.
	 * @param parameters
	 *            The input parameters for the API. Represented by a set of name/value pairs.
	 * @return The API's response if successful, otherwise throw an exception if the response is null.
	 * @throws Exception Let whatever exception is thrown percolate up to the top for it to be handled.
	 */
	public static String remoteOperation(String restAPI, boolean isHttpGet,
		HashMap<String, String> parameters, String userAgent) throws Exception
	{
		return remoteOperation(restAPI, isHttpGet, parameters, userAgent, null, null);
	}
	
	public static String remoteOperation(String restAPI, boolean isHttpGet,
		HashMap<String, String> parameters, String userAgent, String username, String password) throws Exception
	{
		long startTime = System.currentTimeMillis();
		
		if (isHttpGet)
			restAPI = addEncodedParameters(restAPI, parameters);
		
		logger.info("REST API with " + parameters.size() + " encoded parameters [" + restAPI + "]");

		// TODO - Log the parameters for debugging purposes.
		
		URL url = new URL(restAPI);

		HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();

		if (userAgent != null)
			httpConnection.setRequestProperty("User-Agent", userAgent);
		
		// Use Basic Authentication?
		if (username != null)
			{
			if (password == null)
				password = "";
			
			String credentials = username + ":" + password;
			
			String encodedStr = new String(Base64.encodeBase64(credentials.getBytes()));

			httpConnection.setRequestProperty("Authorization", "Basic " + encodedStr);
			}
		
		if (isHttpGet)
			httpConnection.connect();
		else
			{
			httpConnection.setDoOutput(true);
			httpConnection.setRequestMethod("POST");
			httpConnection.setRequestProperty("Content-Type", "application/json");
			//httpConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			doPost(httpConnection.getOutputStream(), parameters);
			}

		String xmlResponse;
		
		if (httpConnection.getResponseCode() == 200)
			xmlResponse = getText(httpConnection.getInputStream());
		else
			xmlResponse = getText(httpConnection.getErrorStream());
		
		if (xmlResponse == null)
			logger.warn("No response from " + restAPI);
		
		logger.info("remoteOperation response in " + (System.currentTimeMillis() - startTime) + " msecs:\n" + xmlResponse);
		
		return xmlResponse;
	}

	public static void doPost(OutputStream outputStream, HashMap<String, String> parameters)
	{
		if (parameters == null)
			return;

		try
			{
			DataOutputStream printout;

			printout = new DataOutputStream(outputStream);

			int i = 0;

			StringBuffer content = new StringBuffer();

			for (String parameter : parameters.keySet())
				{
				if (i > 0)
					content.append("&");

				// If there's no parameter, then just append the content.
				if (parameter.equals(NO_PARAMETER))
					content.append(parameters.get(parameter));
				else
					content.append(parameter).append("=").append(URLEncoder.encode(parameters.get(parameter), "UTF-8"));

				i++;
				}

			// Don't log anything if it contains a password. 
			
			if (content.toString().contains("password") == false)
				{
				if (content.length() > 1024)
					logger.info("Posting " + content.length() + " bytes of content");
				else
					logger.info("Posting " + content.toString());
				}
			
			byte[] bytes = content.toString().getBytes("UTF-8");

			printout.write(bytes, 0, bytes.length);

			printout.flush();

			printout.close();
			}
		catch (Exception e)
			{
			e.printStackTrace();
			}
	}

	public static String addEncodedParameters(String restAPI, HashMap<String, String> parameters) throws Exception
	{
		if (parameters == null)
			return restAPI;

		boolean firstParameter = true;

		StringBuffer strbuf = new StringBuffer(restAPI);

		for (String parameter : parameters.keySet())
			{
			if (firstParameter)
				{
				strbuf.append("?");
				firstParameter = false;
				}
			else
				strbuf.append("&");

			strbuf.append(parameter).append("=").append(URLEncoder.encode(parameters.get(parameter), "UTF-8"));
			}

		return strbuf.toString();
	}

	public static String getText(InputStream inputStream)
	{
		String response = null;

		try
			{
			if (inputStream == null)
				return null;
			
			BufferedReader responseReader = new BufferedReader(new InputStreamReader(inputStream));

			StringWriter responseBuffer = new StringWriter();

			PrintWriter stdout = new PrintWriter(responseBuffer);

			for (String line; (line = responseReader.readLine()) != null;)
				stdout.println(line);

			responseReader.close();

			response = responseBuffer.toString();
			}
		catch (Exception e)
			{
			e.printStackTrace();
			}

		return response.trim(); // Remove trailing newline.
	}	
}