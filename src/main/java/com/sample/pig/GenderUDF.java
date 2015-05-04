package com.sample.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class GenderUDF extends EvalFunc<String> {
		final Logger log = LoggerFactory.getLogger(GenderUDF.class);
	   GenderByName finder = new GenderByName();	
	   @Override
	   public String exec(Tuple tuple)throws IOException {
		   // returns a gender by first name
		   String gender ="UNKNOWN";
		   
	       if(tuple ==null|| tuple.size()<1) {
	    	   System.out.println("GenderfIdentifierUDF: requires one input parameter.");
	    	   log.error("tuple is null");
	       }
	       String firstname = (String)tuple.get(0);
	       try{
	    	   log.info("getting the first name {}", firstname);	    	   
	    	   if (firstname.contains(" ")) {
	    		   log.warn("no compound names {} will take first part of the name", firstname);
	    		   firstname =  firstname.split(" ")[0];
	    	   }
	           gender = finder.getGender(firstname);           
	           log.info("getting the first name {} and computing gender as {}", firstname, gender); 
	           return gender;
	        }	       
	       catch(Exception e) {
	    	   log.error("Error is UDF parsung getGender will try another {} ", finder.toString());
	    	   log.error("Error is UDF parsung getGender will try another {} ", firstname);	    
	           throw new IOException(e);
	        }	       
	    }


	   @Override
	   public Schema outputSchema(Schema input)
	    {
	       return input;
	    }
	

	}
