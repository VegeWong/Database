package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    private int globCnt;
    private HashMap<Field, Integer> table;
    private ArrayList<Tuple> tuples;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        if (what != Op.COUNT)
            throw new IllegalArgumentException();

        globCnt = 0;
        table = new HashMap<Field, Integer>();
        tuples = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField fd = (StringField) tup.getField(afield);
        assert fd.getType() == Type.STRING_TYPE;
        if (gbfieldtype == null) {
            globCnt += 1;
        }
        else {
            Field idenFd = tup.getField(gbfield);
            Integer r = table.get(idenFd);
            if (r == null) {
                r = new Integer(1);
                table.put(idenFd, r);
            }
            else {
                r += 1;
                table.put(idenFd, r);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        tuples = new ArrayList<Tuple>();
        TupleDesc td;
        if (gbfieldtype == null) {
            Type[] typeArr = new Type[]{Type.INT_TYPE};
            String[] nameArr = new String[]{"aggregateValue"};
            td = new TupleDesc(typeArr, nameArr);
            Tuple tp = new Tuple(td);
            tp.setField(0, new IntField(globCnt));
            tuples.add(tp);
        }
        else {
            Type[] typeArr = new Type[]{gbfieldtype, Type.INT_TYPE};
            String[] nameArr = new String[]{"groupValue", "aggregateValue"};
            td = new TupleDesc(typeArr, nameArr);
            Tuple tp;
            for (Map.Entry<Field, Integer> e : table.entrySet()) {
                tp = new Tuple(td);
                tp.setField(0, e.getKey());
                tp.setField(1, new IntField(e.getValue()));
                tuples.add(tp);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
