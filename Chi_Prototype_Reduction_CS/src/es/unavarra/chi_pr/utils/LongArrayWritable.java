/*
 * Copyright (C) 2015 Mikel Elkano Ilintxeta
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package es.unavarra.chi_pr.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class LongArrayWritable implements WritableComparable<LongArrayWritable>, Serializable {
	
	private long[] data;
	private int hash, i, length;
	private String tmpStr;
	private LongArrayWritable o;
	
	/**
     * Default constructor
     */
    public LongArrayWritable() {
    	this.length = 0;
    	hashCode();
    }
    
    /**
     * Constructs a new serializable array of one element
     * @param value input value
     */
    public LongArrayWritable(long value) {
        this.data = new long[]{value};
        this.length = this.data.length;
        hashCode();
    }

	/**
     * Constructs a new serializable array from the input long array
     * @param data input long array
     */
    public LongArrayWritable(long[] values) {
        this.data = new long[values.length];
        for (i = 0; i < values.length; i++)
        	this.data[i] = values[i];
        this.length = this.data.length;
        hashCode();
    }
    
    /*
    @Override
	public int compareTo(LongArrayWritable o) {
    	return new String(data).compareTo(new String(o.getData()));
	}
	*/
    
    @Override
	public int compareTo(LongArrayWritable o) {
    	i = 0;
    	if (data.length == o.getData().length){
			while (i < data.length && data[i]==o.getData()[i])
				i++;
			if (i >= data.length)
				return 0;
		}
    	else if (data.length < o.getData().length){
			while (i < data.length && data[i]==o.getData()[i])
				i++;
			if (i >= data.length)
				return -1;
		}
		else{
			while (i < o.getData().length && data[i]==o.getData()[i])
				i++;
			if (i >= o.getData().length)
				return 1;
		}
		if (data[i] > o.getData()[i])
			return 1;
		else
			return -1;
	}
    
    @Override
	public boolean equals (Object obj){
    	
    	if (obj == this)
    		return true;
    	if (obj == null || obj.getClass() != this.getClass())
    		return false;
    	
    	i = 0;
    	o = (LongArrayWritable)obj;
    	if (data.length == o.getData().length){
			while (i < data.length && data[i]==o.getData()[i])
				i++;
			if (i >= data.length)
				return true;
			else
				return false;
		}
    	else
    		return false;
    	
    }

    /**
     * Returns the long array
     * @return long array
     */
    public long[] getData() {
        return data;
    }
    
    @Override
    public int hashCode(){
    	if (hash == -1){
    		tmpStr = "";
    		for (i = 0; i < length; i++)
    			tmpStr += data[i];
    		hash =  tmpStr.hashCode();
    	}
		return hash;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	
    	hash = in.readInt();
        length = in.readInt();

        data = new long[length];

        for(i = 0; i < length; i++)
            data[i] = in.readLong();
        
    }

    /**
     * Sets the long array
     * @param data input long array
     */
    public void setData(long[] data) {
        this.data = data;
    }

    @Override
    public String toString(){
    	
    	String output = "Values: ";
    	
    	for (i = 0; i < data.length; i++)
    		output += data[i] + " | ";
    	
    	return output;
    	
    }
    
    @Override
    public void write(DataOutput out) throws IOException {

    	out.writeInt(hash);
        out.writeInt(length);

        for(i = 0; i < length; i++)
            out.writeLong(data[i]);
        
    }

}
