package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc _schema;
    private Field[] _fieldAr;
    private RecordId _rid;
    private static final String errMsg_indexOutOfBounds = "Request index out of bounds";
    private static final String errMsg_cannotFindName = "Can not find field with given name";
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        // modified: 2019.4.18
        this._schema = td;
        this._fieldAr = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // modified: 2019.4.18
        return _schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return _rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        _rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        // modified: 2019.4.18
        _fieldAr[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) throws NoSuchElementException {
        // some code goes here
        // modified: 2019.4.18
        if (i < _fieldAr.length)
            return _fieldAr[i];
        else
            throw new NoSuchElementException(errMsg_indexOutOfBounds);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        // modified: 2019.4.18
        if (_fieldAr == null || _fieldAr.length == 0)
            return "";
        StringBuffer sbuffer = new StringBuffer(_fieldAr[0].toString());
        for (int i = 1; i < _fieldAr.length; ++i) {
            sbuffer.append("\t");
            sbuffer.append(_fieldAr[i].toString());
        }
        return sbuffer.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        // modified: 2019.4.18
        class FieldIterator implements Iterator<Field> {
            private int _curIndex;
            private Field[] _fdPtr;

            public FieldIterator(Field[] ptr) {
                _fdPtr = ptr;
                _curIndex = 0;
            }

            @Override
            public boolean hasNext() {
                return  _curIndex < _fdPtr.length;
            }

            @Override
            public Field next() {
                return _fdPtr[_curIndex++];
            }
        }
        return new FieldIterator(this._fieldAr);
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        // modified: 2019.4.18
        _schema = td;
        _fieldAr = new Field[td.numFields()];
    }
}
