package com.sample.pig;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class GenderByName {
	final Logger log = LoggerFactory.getLogger(GenderByName.class);
	
	public static void main(String args[])
	{
		GenderByName gender = new GenderByName();
		gender.getGender("john");
	}
	public String getGender(String firstname)
	{
		String gender= "UNKNOWN";
		StringBuffer baseUrl = new StringBuffer("https://api.genderize.io/?name=");
		baseUrl.append(firstname);
		String url = baseUrl.toString();
		gender = parser(url, firstname);
		log.info("gender {} for name {} ", gender, firstname);
		return gender;
	}

	private String parser(String url, String name)
	{		
	    HttpClient client = HttpClientBuilder.create().build();
	    HttpGet request = new HttpGet(url);
	    request.addHeader("accept", "application/json");
	    HttpResponse response = null;
	    String json = null;
	    String result = null;
	    try {
		    response = client.execute(request);
	        json = IOUtils.toString(response.getEntity().getContent());
	        
            
            JSONObject object = new JSONObject(json);
            log.info("response {}", object.toString());
            String gStr = "unknown";
            try {
            	gStr = object.getString("gender");
            }
            catch (JSONException jsonExc){
            	log.info("Retrieving the response is null");
            	return "unknown";
            }
            if (gStr == null){
            	log.info("gender {} for name {} ", null, name);
            	gStr = "unknown";
            }
            else {
	            if (gStr.startsWith("f"))
	            	return result="female";
	            else if (gStr.startsWith("m"))
	            	return result= "male";
	            else 
	            	return result = "unknown";
	//	        JSONArray array = new JSONArray(json);
	//	        for (int i = 0; i < array.length(); i++) {
	//	        	JSONObject object = array.getJSONObject(i);    
	//	        }
            }
        }
	    catch (IOException exc) {
	    	log.error("Response error" + exc);
	    }
		//https://api.genderize .io/?name=peter
		return result;
	}
}
