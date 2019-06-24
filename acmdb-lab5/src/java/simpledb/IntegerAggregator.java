package simpledb;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    class Rec
    {
        public int cnt;
        public int val;

        public Rec() {
            cnt = 0;
            val = 0;
        }

        public Rec(int v) {
            cnt = 1;
            val = v;
        }
    }

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    private Rec globVal;
    private HashMap<Field, Rec> table;
    private ArrayList<Tuple> tuples;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (gbfield == Aggregator.NO_GROUPING)
            assert gbfieldtype == null;

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.globVal = null;
        this.table = new HashMap<Field, Rec>();
        this.tuples = null;
    }

    private void merge(Rec r, Integer i) {
        r.cnt += 1;
        switch (op) {
            case AVG: case SUM: r.val += i; break;
            case MAX: r.val = i > r.val? i : r.val; break;
            case MIN: r.val = i > r.val? r.val : i; break;
            case COUNT: r.val += 1; break;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField fd = (IntField) tup.getField(afield);
        assert fd.getType() == Type.INT_TYPE;
        if (gbfieldtype == null) {
            if (globVal == null) {
                if (op == Op.COUNT)
                    globVal = new Rec(1);
                else
                    globVal = new Rec(fd.getValue());
            }
            else
                merge(globVal, fd.getValue());
        }
        else {
            Field idenFd = tup.getField(gbfield);
            Rec r = table.get(idenFd);
            if (r == null) {
                if (op == Op.COUNT)
                    r = new Rec(1);
                else
                    r = new Rec(fd.getValue());
                table.put(idenFd, r);
            }
            else
                merge(r, fd.getValue());
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        tuples = new ArrayList<Tuple>();
        TupleDesc td;
        if (gbfieldtype == null) {
            Type[] typeArr = new Type[]{Type.INT_TYPE};
            String[] nameArr = new String[]{"aggregateValue"};
            td = new TupleDesc(typeArr, nameArr);
            int _val = globVal.val;
            if (op == Op.AVG)
                _val = globVal.val / globVal.cnt;
            Tuple tp = new Tuple(td);
            tp.setField(0, new IntField(_val));
            tuples.add(tp);
        }
        else {
            Type[] typeArr = new Type[]{gbfieldtype, Type.INT_TYPE};
            String[] nameArr = new String[]{"groupValue", "aggregateValue"};
            td = new TupleDesc(typeArr, nameArr);
            Tuple tp;
            for (Map.Entry<Field, Rec> e : table.entrySet()) {
                tp = new Tuple(td);
                tp.setField(0, e.getKey());
                Rec v = e.getValue();
                int _val = v.val;
                if (op == Op.AVG)
                    _val = v.val / v.cnt;
                tp.setField(1, new IntField(_val));
                tuples.add(tp);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
