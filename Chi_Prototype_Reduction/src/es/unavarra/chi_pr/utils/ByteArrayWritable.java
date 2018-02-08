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

/**
 * Implementation of a serializable byte array
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class ByteArrayWritable implements WritableComparable<ByteArrayWritable>, Serializable {
    
	private byte[] bytes;
	private int hash, i, length;
	private ByteArrayWritable o;
	
	/**
     * Default constructor
     */
    public ByteArrayWritable() {
    	this.length = 0;
    	hashCode();
    }
    
    /**
     * Constructs a new serializable array of one element
     * @param data input byte
     */
    public ByteArrayWritable(byte data) {
        this.bytes = new byte[]{data};
        this.length = this.bytes.length;
        hashCode();
    }

	/**
     * Constructs a new serializable array from the input byte array
     * @param data input byte array
     */
    public ByteArrayWritable(byte[] data) {
    	this.bytes = new byte[data.length];
        for (i = 0; i < data.length; i++)
        	this.bytes[i] = data[i];
        this.length = this.bytes.length;
        hashCode();
    }
    
    /*
    @Override
	public int compareTo(ByteArrayWritable o) {
    	return new String(bytes).compareTo(new String(o.getBytes()));
	}
	*/
    
    @Override
	public int compareTo(ByteArrayWritable o) {
    	i = 0;
    	if (bytes.length == o.getBytes().length){
			while (i < bytes.length && bytes[i]==o.getBytes()[i])
				i++;
			if (i >= bytes.length)
				return 0;
		}
    	else if (bytes.length < o.getBytes().length){
			while (i < bytes.length && bytes[i]==o.getBytes()[i])
				i++;
			if (i >= bytes.length)
				return -1;
		}
		else{
			while (i < o.getBytes().length && bytes[i]==o.getBytes()[i])
				i++;
			if (i >= o.getBytes().length)
				return 1;
		}
		if (bytes[i] > o.getBytes()[i])
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
    	o = (ByteArrayWritable)obj;
    	if (bytes.length == o.getBytes().length){
			while (i < bytes.length && bytes[i]==o.getBytes()[i])
				i++;
			if (i >= bytes.length)
				return true;
			else
				return false;
		}
    	else
    		return false;
    	
    }

    /**
     * Returns the byte array
     * @return byte array
     */
    public byte[] getBytes() {
        return bytes;
    }
    
    @Override
    public int hashCode(){
    	if (hash == -1)
    		hash =  new String(bytes).hashCode();
		return hash;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	
    	hash = in.readInt();
        length = in.readInt();

        bytes = new byte[length];

        for(i = 0; i < length; i++)
            bytes[i] = in.readByte();
        
    }

    /**
     * Sets the byte array
     * @param data input byte array
     */
    public void setData(byte[] data) {
        this.bytes = data;
    }

    @Override
    public String toString(){
    	
    	String output = "Bytes: ";
    	
    	for (i = 0; i < bytes.length; i++)
    		output += bytes[i] + " | ";
    	
    	return output;
    	
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
    	
        out.writeInt(hash);
        out.writeInt(length);

        for(i = 0; i < length; i++)
            out.writeByte(bytes[i]);
        
    }
    
}
