package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op op;
    private DbIterator itr;
    private TupleDesc td;
    private TupleDesc resTd;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.op = aop;
        this.td = child.getTupleDesc();
        this.resTd = null;
        this.itr = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    // some code goes here
        if (gfield >= 0)
	        return gfield;
        else
            return Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	    // some code goes here
        if (gfield >= 0)
	        return child.getTupleDesc().getFieldName(gfield);
        else
            return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    // some code goes here
	    return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    // some code goes here
	    return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    // some code goes here
	    return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        child.open();
        super.open();

        Aggregator agg;
        Type gbtype = gfield < 0? null : td.getFieldType(gfield);
        if (td.getFieldType(afield) == Type.STRING_TYPE)
            agg = new StringAggregator(groupField(), gbtype, afield, op);
        else
            agg = new IntegerAggregator(groupField(), gbtype, afield, op);
        while (child.hasNext())
            agg.mergeTupleIntoGroup(child.next());

        itr = agg.iterator();
        itr.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    // some code goes here
        if (itr.hasNext())
            return itr.next();
        else
	        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    // some code goes here
        child.rewind();
        itr.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    // some code goes here
        if (resTd != null)
            return resTd;
        Type[] typeArr;
        String[] nameArr;
        if (gfield < 0) {
            typeArr = new Type[] {td.getFieldType(afield)};
            nameArr = new String[] {
                    "aggName("+nameOfAggregatorOp(op)+") ("+td.getFieldName(afield)+")"
            };
        }
        else {
            typeArr = new Type[] {td.getFieldType(gfield),
                    td.getFieldType(afield)};
            nameArr = new String[] {
                    "groupbyFieldName("+td.getFieldName(gfield)+")",
                    "aggName("+nameOfAggregatorOp(op)+") ("+td.getFieldName(afield)+")"
            };
        }
        resTd = new TupleDesc(typeArr, nameArr);
	    return resTd;
    }

    public void close() {
	    // some code goes here
        child.close();
        itr.close();
    }

    @Override
    public DbIterator[] getChildren() {
	    // some code goes here
	    return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    // some code goes here
        child = children[0];
        resTd = null;
        td = child.getTupleDesc();
        itr = null;
    }
    
}
