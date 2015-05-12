package com.sample.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * -- subspace convergence
( ABCs, 1)
( Anchors & Stripes, 2)
( Balloons, 3)
( Baseballs, 4)
( Bicycles, 5)
( Bloom, 6)
( Blue Gingham, 7)
( Boomboxes, 8)
( Bow Ties, 9)
( Bumble Bees, 10)
( Butterflies, 11)
( Chevron, 12)
( Comic Book, 13)
( Confetti Hearts, 14)
( Daisies, 15)
( Giraffes, 16)
( Guitars, 17)
( Leopard, 18)
( Skulls, 19)
( Strawberries, 20)
( White, 21)
 * @author verak
 *
 */
public class BundleToVecUDF extends EvalFunc<Tuple>{
	final Logger log = LoggerFactory.getLogger(BundleToVecUDF.class);
	HashMap<String, Integer> printSpace = new HashMap<String, Integer>();
	Tuple outTuple = TupleFactory.getInstance().newTuple(20);
	@Override
   public Tuple exec(Tuple tuple)throws IOException {
	   // returns a gender by first name
	   Tuple t = TupleFactory.getInstance().newTuple();
	   
       if(tuple ==null|| tuple.size()<1) {    	   
    	   log.error("tuple is null");
       }
       else {
    	   log.error("tuple is NOT NULL !!!!!!! ");
    	   populateBundleSpace();
    	   t = populateTuple(tuple);
       }
       return t;
        
	}	       
	private void populateBundleSpace()
	{
		printSpace.put("ABCs", 1);
		printSpace.put("Anchors & Stripes", 2);
		printSpace.put("Balloons", 3);
		printSpace.put("Baseballs", 4);
		printSpace.put("Bicycles", 5);
		printSpace.put("Bloom", 6);
		printSpace.put("Blue Gingham", 7);
		printSpace.put("Boomboxes", 8);
		printSpace.put("Bow Ties", 9);
		printSpace.put("Bumble Bees", 10);
		printSpace.put("Chevron", 11);
		printSpace.put("Comic Book", 12);
		printSpace.put("Confetti Hearts", 13);
		printSpace.put("Daisies", 14);
		printSpace.put("Giraffes", 15);
		printSpace.put("Guitars", 16);
		printSpace.put("Leopard", 17);
		printSpace.put("Skulls", 18);
		printSpace.put("Strawberries", 19);
		printSpace.put("White", 20);
		
		// populate empty tuple for return 
		for (int i=0; i<20; ++i)
    	{
			try {
				outTuple.set(i, 0);
			}
			catch(ExecException exc)
			{
				log.error("Unable to construct a default tuple for return." + exc);
			}
    		
    	}
	}
    private Tuple populateTuple(Tuple inTuple)
    {    	
    	for (int i=0; i<inTuple.size(); ++i)
    	{
    		String curPrint = "unknown";
//    		List<String> prints = new ArrayList<String>();
    		try{
    			Object values = inTuple.get(0);
                if (values instanceof DataBag)
                {
                	DataBag db = (DataBag )values;
                	Iterator iter = db.iterator();
                	while (iter.hasNext())
                	{
                		curPrint = (String)iter.next();
                		int index =  this.printSpace.get(curPrint);
                		log.info("Looking at print {}", curPrint);
                		log.info("Looking at index {}", index);
            			outTuple.set(index, 1);
                	}
                }
                else if (values instanceof Tuple)
                {
                	Tuple db = (Tuple)values;
                	Iterator iter = db.iterator();
                	while (iter.hasNext())
                	{
                		curPrint = (String)iter.next();
                		System.out.println("In Vector UDF " + curPrint);
                		log.info("Looking at {}", curPrint);
            			outTuple.set(this.printSpace.get(curPrint), 1);
                	}
                }
                else {
                	outTuple.set(0, -1);
                }
                
//    			curPrint = (String)inTuple.get(i);
    			
    		}
    		catch(ExecException exc)
    		{
    			log.error("Unable to construc a vector" + exc);
    		}
    	}
    	return outTuple;
    }    
}

	 
