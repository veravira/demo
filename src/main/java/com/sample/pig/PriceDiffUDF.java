package com.sample.pig;


import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PriceDiffUDF extends EvalFunc<Integer>{
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	public Integer exec(Tuple tuple) throws IOException {
        // expect price
        if (tuple == null || tuple.size()<1) {
        	System.out.println("PriceDiffUDF: requires one input parameter.");        	
        }
        try {
//        	System.out.println("PriceDiffUDF: " + tuple.toString());
            Integer givenPrice = (Integer)tuple.get(0);
            Integer curPrice = (Integer)tuple.get(1);
            int diff = Math.abs(givenPrice-curPrice); 
            return diff;
        }
        catch (Exception e) {
            throw new IOException("PriceDiffUDF: caught exception processing input.", e);
        }
    }    		
    
}

