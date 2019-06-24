package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private static final int BLOCK_SIZE = 100000;

    private JoinPredicate p;
    private DbIterator outer;
    private DbIterator inner;
    private TupleDesc td;
    private List<Tuple> filteredTuples;
    private TupleIterator filteredTupItr;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        outer = child1;
        inner = child2;
        filteredTuples = new LinkedList<Tuple>();
        filteredTupItr = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        int index = p.getField1();
        return outer.getTupleDesc().getFieldName(index);
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        int index = p.getField2();
        return inner.getTupleDesc().getFieldName(index);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(outer.getTupleDesc(),
                inner.getTupleDesc());
    }

    private void nestedLoopJoin() throws DbException,
            TransactionAbortedException {
        int len1 = outer.getTupleDesc().numFields();
        int len2 = inner.getTupleDesc().numFields();
        Tuple[] cacheOfOuterTable = new Tuple[BLOCK_SIZE];
        outer.rewind();
        int cnt = 0;
        Tuple in, out;
        while (true) {
            while (cnt < BLOCK_SIZE && outer.hasNext())
                cacheOfOuterTable[cnt++] = outer.next();

            if (cnt == 0) {
                filteredTupItr = new TupleIterator(td, filteredTuples);
                return;
            }

            for (int i = 0; i < cnt; ++i) {
                out = cacheOfOuterTable[i];
                inner.rewind();
                while (inner.hasNext()) {
                    in = inner.next();
                    if (p.filter(out, in)) {
                        Tuple tp = new Tuple(td);
                        int ind, now;
                        for (ind = 0, now = 0; now < len1; ++ind, ++now)
                            tp.setField(ind, out.getField(now));
                        for (now = 0; now < len2; ++now, ++ind)
                            tp.setField(ind, in.getField(now));
                        filteredTuples.add(tp);
                    }
                }
            }
            cnt = 0;
        }
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        outer.open();
        inner.open();

        TupleDesc td1 = outer.getTupleDesc();
        TupleDesc td2 = inner.getTupleDesc();
        td = TupleDesc.merge(td1, td2);
        nestedLoopJoin();
        filteredTupItr.open();
    }

    public void close() {
        // some code goes here
        inner.close();
        outer.close();
        super.close();
        filteredTuples.clear();
        filteredTupItr = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        inner.rewind();
        outer.rewind();
        filteredTupItr.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (filteredTupItr.hasNext())
            return filteredTupItr.next();
        else
            return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {
                outer,
                inner
        };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        outer = children[0];
        inner = children[1];
        filteredTuples.clear();
        filteredTupItr = null;
    }

}
