package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.List;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    int [] histogram;
    private int min;
    private int max;
    private int width;
    private int ntup;
    private int zeroOffset; // 这是为了对付负值的;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        if(buckets > max - min + 1) buckets = max - min + 1;
        histogram = new int[buckets];
        // 如果出现负值了，我们需要将整段 "搬到" 正值区
        if(min < 0){
            zeroOffset = -min;
        }
        this.min = min + zeroOffset;
        this.max = max + zeroOffset;
        width = (max - min + 1) / buckets;
        ntup = 0;
    }

    private int getBucket(int v){
        v = v + zeroOffset;
        if(v < min || v > max) return -1;
        return Math.min((v - min) / width, histogram.length - 1); // 这边不要 +1，考虑 1,2,3分成3个桶, v == 1, 那么 bucket = (1 - 1) / 1 = 0 正好
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
       // 我们要计算一下v该落在哪个桶里:
        int bucket = getBucket(v);
        if(bucket != -1){
            histogram[bucket]++;
            ntup++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    // LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op){
            case EQUALS:
                return dealEquals(v);
            case GREATER_THAN:
                return dealMore(v);
            case LESS_THAN:
                return 1 - dealMore(v) - dealEquals(v);
            case LESS_THAN_OR_EQ:
                return 1- dealMore(v);
            case GREATER_THAN_OR_EQ:
                return dealMore(v) + dealEquals(v);
            case NOT_EQUALS:
                return 1 - dealEquals(v);
            default:
                return -1;
        }
    }
    // 做好 = 和 > 就好了
    private double dealEquals(int v){
        int bucket = getBucket(v);
        if(bucket == -1) return 0;
        return histogram[bucket] * 1.0 / (width * ntup);
    }

    private double dealMore(int v){
        int bucket = getBucket(v), nxtBucket = getBucket(v + width);
        if(bucket == -1){
            if(v < min) return 1;
            else return 0;
        }
        int rightBound = nxtBucket == -1 ? max : (min + width * nxtBucket);
        double bPart = (rightBound - v - 1) * 1.0 / width; // 注意这里要 -1 ，因为rightBound算下一组的;
        double sum = bPart * histogram[bucket];
        bucket ++;
        while (bucket < histogram.length){
            sum += histogram[bucket];
            bucket++;
        }
        return sum / ntup;
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        System.out.println("avgSelectivity: START!!!");
        return ntup * 1.0 / (max - min + 1);
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder builder = new StringBuilder("IntHistogram: \n  max: " + max + "  ||  min: " + min +"\n");
        for (int i = 0; i < histogram.length; i++) {
            int k = histogram[i];
            builder.append("bucket").append(i).append("  ");
            builder.append(min + width * i).append("-").append(min + width * (i + 1)).append(":  ").append(k);
            builder.append("\n");
        }
        return builder.toString();
    }
}
