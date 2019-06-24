package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private DbFile _file;
    private DbFileIterator _fitr;
    private String _alias;
    private TransactionId _tid;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        _file = Database.getCatalog().getDatabaseFile(tableid);
        _fitr = _file.iterator(tid);
        if (tableAlias == null)
            _alias = "null";
        else
            _alias = tableAlias;
        _tid = tid;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(_file.getId());
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return _alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        _fitr.close();
        _alias = tableAlias;
        _file =Database.getCatalog().getDatabaseFile(tableid);
        _fitr = _file.iterator(_tid);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            _fitr.open();
        } catch (DbException e) {
            throw e;
        } catch (TransactionAbortedException e) {
            throw e;
        }
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc _td = _file.getTupleDesc();
        int _len = _td.numFields();
        Type[] _type = new Type[_len];
        String[] _fieldAr = new String[_len];
        for (int i = 0; i < _td.numFields(); ++i) {
            _type[i] = _td.getFieldType(i);
            StringBuffer sbuffer = (new StringBuffer(_alias)).append(".");
            _fieldAr[i] = sbuffer.append(_td.getFieldName(i)).toString();
        }
        return new TupleDesc(_type, _fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        try {
            return _fitr.hasNext();
        } catch (DbException e) {
            throw e;
        } catch (TransactionAbortedException e) {
            throw e;
        }
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        try {
            return _fitr.next();
        } catch (DbException e) {
            throw e;
        } catch (TransactionAbortedException e) {
            throw e;
        } catch (NoSuchElementException e) {
            throw e;
        }
    }

    public void close() {
        // some code goes here
        _fitr.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        try {
            _fitr.rewind();
        } catch (DbException e) {
            throw e;
        } catch (TransactionAbortedException e) {
            throw e;
        } catch (NoSuchElementException e) {
            throw e;
        }
    }
}
