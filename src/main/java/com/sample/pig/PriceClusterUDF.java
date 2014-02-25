package com.sample.pig;


import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PriceClusterUDF extends EvalFunc<Integer>{
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	public Integer exec(Tuple tuple) throws IOException {
        // expect price
        if (tuple == null || tuple.size()<1) {
        	System.out.println("PriceGrpUDF: requires one input parameter.");        	
        }
        try {
            Integer first = (Integer)tuple.get(0);            
            int brucket = generateClusterNumber(first); 
            return brucket;
        }
        catch (Exception e) {
            throw new IOException("PriceGrpUDF: caught exception processing input.", e);
        }
    }
    		
    		
    private int generateClusterNumber(Integer price) {
    	int num = -1;
    	if(price >= 150000)
    	{
    		num = 150000;
    	}
    	else if (price>=100000 && price<150000)
    	{
    		num = 100000;
    	}
    	else if (price>=70000 && price<100000)
    	{
    		num = 70000;
    	}
    	else if (price<70000 && price>=60000)
    	{
    		num = 60000;
    	}
    	else if (price<60000 && price>=50000)
    	{
    		num = 50000;
    	}
    	else if (price>=47000 && price<50000)
    	{
    		num = 47000;
    	}
    	else if (price>=45000 && price<47000)
    	{
    		num = 45000;
    	}
    	
    	else if (price>=43000 && price<45000)
    	{
    		num = 43000;
    	}
    	else if (price>=40000 && price<43000)
    	{
    		num = 40000;
    	}
    	else if (price>=35000 && price<40000)
    	{
    		num = 35000;
    	}
    	else if (price>=30000 && price<35000)
    	{
    		num = 30000;
    	}
    	else if (price>=25000 && price<30000)
    	{
    		num = 25000;
    	}
    	else if (price>=23500 && price<25000)
    	{
    		num = 23500;
    	}
    	else if (price>=22000 && price<23500)
    	{
    		num = 22000;
    	}
    	else if (price>=20000 && price<22000)
    	{
    		num = 20000;
    	}
    	else if (price>=17000 && price<20000)
    	{
    		num = 17000;
    	}
    	else if (price>=15000 && price<17000)
    	{
    		num = 15000;
    	}
    	else if (price>=13000 && price<15000)
    	{
    		num = 13000;
    	}
    	else if (price>=13000 && price<15000)
    	{
    		num = 13000;
    	}
    	else if (price<13000)
    	{
    		num = 0;
    	}
    	return num;
    }

}

