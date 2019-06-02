package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int numBucket;
    private int minVal;
    private int maxVal;

    private int width;
    private double[] heights;
    private int totalNum;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.minVal = min;
        this.maxVal = max;
        if (buckets > max - min + 1)
            this.numBucket = max - min + 1;
        else
            this.numBucket = buckets;
        this.width = (int) Math.ceil((double) (max - min + 1) / numBucket);
        this.heights = new double[numBucket];
        this.totalNum = 0;
    }

    private int getBucketIndex(int t) {
        int index = (int) ((t - this.minVal) / this.width);
        if (index == numBucket && t <= maxVal) {
            --index;
        }
        return index;
    }

    private int getBucketWidth(int ind) {
        if (ind + 1 == numBucket)
            return maxVal - minVal + 1 - width * ind;
        else
            return width;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = getBucketIndex(v);
        this.heights[index] += 1;
        this.totalNum += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int index = getBucketIndex(v);
        int wid = getBucketWidth(index);


        int add = 0;
        switch (op) {
            case EQUALS: {
                if (index < 0 || index >= numBucket)
                    return 0;
                return heights[index] / (wid * totalNum);
            }
            case LESS_THAN_OR_EQ: add = 1;
            case LESS_THAN: {
                if (index < 0)
                    return 0;
                else if (index >= numBucket)
                    return 1;

                double inBucket = (v - minVal - index * width + add) * heights[index] / wid;
                double outBucket = 0;
                for (int i = 0; i < index; ++i)
                    outBucket += heights[i];
                return (inBucket + outBucket) / totalNum;
            }
            case GREATER_THAN_OR_EQ: add = 1;
            case GREATER_THAN: {
                if (index < 0)
                    return 1;
                else if (index >= numBucket)
                    return 0;

                int inBucketCnt = index == numBucket - 1 ? maxVal - v :
                        minVal + (index + 1) * width - v - 1;
                inBucketCnt += add;
                double inBucket = inBucketCnt * heights[index] / wid;
                double outBucket = 0;
                for (int i = index + 1; i < numBucket; ++i)
                    outBucket += heights[i];
                return (inBucket + outBucket) / totalNum;
            }
            case NOT_EQUALS: {
                if (index < 0 || index >= numBucket)
                    return 1;
                return 1 - heights[index] / (wid * totalNum);
            }
            case LIKE: {

            }
        }
        return 0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "IntHistogram:" +
                "buckets="+String.valueOf(numBucket) +
                "min="+String.valueOf(minVal) +
                "max="+String.valueOf(maxVal);
    }
}
