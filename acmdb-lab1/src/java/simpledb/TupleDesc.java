package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @attribute _fieldAr
     *                  Array of TDItem, describe the tuple
     * @attribute _defaultName
     *                  Default name for unamed fields, used as a prefix
     *                  e.g. unamed1, unamed2
     * */
    private TDItem[] _itemAr;
    private static final String _defaultName = "null";
    private static final String errMsg_indexOutOfBounds = "Request index out of bounds";
    private static final String errMsg_cannotFindName = "Can not find field with given name";
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        // modified: 2019.4.18
        class TDItemIterator implements Iterator<TDItem> {

            private int _curIndex;
            private TDItem[] _arPtr;

            public TDItemIterator(TDItem[] _array) {
                _arPtr = _array;
                _curIndex = 0;
            }

            @Override
            public boolean hasNext() {
                return _curIndex < _arPtr.length;
            }

            @Override
            public TDItem next() {
                return _arPtr[_curIndex++];
            }
        }

        return new TDItemIterator(_itemAr);
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        // modified: 2019.4.18
        _itemAr = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; ++i) {
            if (i < fieldAr.length && fieldAr[i] != null)
                _itemAr[i] = new TDItem(typeAr[i], fieldAr[i]);
            else
                _itemAr[i] = new TDItem(typeAr[i], _defaultName);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        // modified: 2019.4.18
        _itemAr = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; ++i) {
            _itemAr[i] = new TDItem(typeAr[i], _defaultName);
        }
    }

    private TupleDesc(TDItem[] _itemAr) {
        this._itemAr = _itemAr;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        // modified: 2019.4.18
        return this._itemAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        // modified: 2019.4.18
        if (i < this.numFields())
            return this._itemAr[i].fieldName;
        else throw new NoSuchElementException(errMsg_indexOutOfBounds);
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        // modified: 2019.4.18
        if (i < this.numFields())
            return this._itemAr[i].fieldType;
        else throw new NoSuchElementException(errMsg_indexOutOfBounds);
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        // modified: 2019.4.18
        for (int i = 0; i < _itemAr.length; ++i) {
            if (_itemAr[i].fieldName.equals(name))
                return i;
        }
        throw new NoSuchElementException(errMsg_cannotFindName);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        // modified: 2019.4.18
        int sz = 0;
        for (int i = 0; i < _itemAr.length; ++i) {
            sz += _itemAr[i].fieldType.getLen();
        }
        return sz;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        // modified: 2019.4.18
        TDItem[] _mitemAr = new TDItem[td1._itemAr.length + td2._itemAr.length];
        System.arraycopy(td1._itemAr, 0, _mitemAr, 0, td1._itemAr.length);
        System.arraycopy(td2._itemAr, 0, _mitemAr, td1._itemAr.length, td2._itemAr.length);
        return new TupleDesc(_mitemAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        // modified: 2019.4.18
        if (this == o)
            return true;
        if (!(o instanceof TupleDesc))
            return false;
        if (this._itemAr.length != ((TupleDesc) o)._itemAr.length)
            return false;

        for (int i = 0; i < this._itemAr.length; ++i)
            if (this._itemAr[i].fieldType != ((TupleDesc) o)._itemAr[i].fieldType)
                return false;
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        // modified: 2019.4.18
        if (_itemAr.length == 0)
            return "";
        StringBuffer sbuffer = new StringBuffer(_itemAr[0].toString());
        for (int i = 1; i < this._itemAr.length; ++i) {
            sbuffer.append(",");
            sbuffer.append(_itemAr[i].toString());
        }
        return sbuffer.toString();
    }
}
