package es.unavarra.chi_pr.utils;

public class PartialSum {
	
	private long[] categoricalSums; // Counter of occurrences for each categorical value (for categorical variables)
	private Double numericSum; // Partial sum (for numeric variables)
	
	private int i;
	private PartialSum o;
	
	public PartialSum (double numericalSum){
		this.numericSum = numericalSum;
		this.categoricalSums = null;
	}
	
	public PartialSum (long[] categoricalSums){
		this.numericSum = null;
		this.categoricalSums = categoricalSums;
	}
	
	 @Override
	public boolean equals (Object obj){
    	
    	if (obj == this)
    		return true;
    	if (obj == null || obj.getClass() != this.getClass())
    		return false;
    	
    	i = 0;
    	o = (PartialSum)obj;
    	
    	if (numericSum != null && o.numericSum != null && numericSum.doubleValue() == o.numericSum.doubleValue())
    		return true;
    	else if (categoricalSums != null && o.categoricalSums != null && categoricalSums.length == o.categoricalSums.length){
    		for (i = 0; i < categoricalSums.length; i++)
    			if (categoricalSums[i] != o.categoricalSums[i])
    				return false;
    		return true;
    	}
    	else
    		return false;
    	
	 }
	
	public double getNumericSum() {
		return numericSum;
	}

	public void setNumericSum(double numericSum) {
		this.numericSum = numericSum;
	}

	public long[] getCategoricalSums() {
		return categoricalSums;
	}

	public void setCategoricalSums(long[] categoricalSums) {
		this.categoricalSums = categoricalSums;
	}
	
	/**
	 * Returns adds the given PartialSum to the current one
	 * @param partialSum PartialSum to be added
	 */
	public void sum(PartialSum partialSum){
		if (partialSum != null){
			if (numericSum != null)
				numericSum += partialSum.numericSum;
			else
				for (i = 0; i < categoricalSums.length; i++)
					categoricalSums[i] += partialSum.categoricalSums[i];
		}
	}
	
}
