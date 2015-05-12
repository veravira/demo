package com.sample.pig;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComputeCosineUDF extends EvalFunc<Double>{
	private double result = 0.0d;
	
	final Logger log = LoggerFactory.getLogger(ComputeCosineUDF.class);
	
	public Double exec(Tuple tuple) throws IOException
	{
		return computeCosine(tuple);
	}
	
    private double computeCosine(Tuple inTuple)
    {
    	Tuple t1, t2;
    	for (int i=0; i<inTuple.size(); ++i)
    	{
    		try{
    			Object values = inTuple.get(0);
                if (values instanceof DataBag)
                {
                	DataBag db = (DataBag )values;
                	Iterator iter = db.iterator();
                	if (iter.hasNext()) {
                		Object obj = iter.next();
                		log.info("Looking at bag {}", obj);
                		t1 = (Tuple)obj;
                	}
                	if (iter.hasNext())
                	{
                		Object obj = iter.next();
                		log.info("Looking at bag {}", obj);
                		t2 = (Tuple)obj;
                	}
                }
                else if (values instanceof Tuple)
                {
                	Tuple db = (Tuple)values;
                	Iterator iter = db.iterator();
                	int size =0;
                	while (iter.hasNext())
                	{
                		log.info("Looking at {}, {}", iter.next(), size++);            			
                	}
                }
                else {
                	log.info("What is the TYPE .... ");
                }
    			
    		}
    		catch(ExecException exc)
    		{
    			log.error("Unable to construc a vector" + exc);
    		}
    	}
    	return result;
    }
	
}
