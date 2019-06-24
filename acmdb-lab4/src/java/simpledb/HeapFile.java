package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private int _numPg;
    private File _file;
    private TupleDesc _schema;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        _file = f;
        _schema = td;
        _numPg = (int) (_file.length() / BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return _file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        if (_file != null)
            return _file.getAbsoluteFile().hashCode();
        return 0;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return _schema;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        byte[] _retrievedPage = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile _fptr = new RandomAccessFile(_file, "r");
            _fptr.seek(pid.pageNumber() * BufferPool.getPageSize());
            _fptr.read(_retrievedPage, 0, BufferPool.getPageSize());
            _fptr.close();
            return new HeapPage((HeapPageId) pid, _retrievedPage);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException();
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            RandomAccessFile _fptr = new RandomAccessFile(_file, "rw");
            _fptr.seek(page.getId().pageNumber() * BufferPool.getPageSize());
            _fptr.write(page.getPageData(), 0, BufferPool.getPageSize());
        } catch (FileNotFoundException e) {
            throw new IOException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        _numPg = (int) (_file.length() / BufferPool.getPageSize());
        return _numPg;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPageId pid;
        ArrayList<Page> dirtyPages = new ArrayList<Page>();
        for (int pgNo = 0; pgNo < _numPg; ++pgNo) {
            pid = new HeapPageId(getId(), pgNo);
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid,
                    pid, Permissions.READ_WRITE);
            if (pg.getNumEmptySlots() > 0) {
                pg.insertTuple(t);
                dirtyPages.add(pg);
            }
        }
        if (dirtyPages.size() == 0) {
            pid = new HeapPageId(getId(), _numPg++);
            HeapPage pg = new HeapPage(pid, HeapPage.createEmptyPageData());
            writePage(pg);
            pg = (HeapPage) Database.getBufferPool().getPage(tid,
                    pid, Permissions.READ_WRITE);
            assert pg.getNumEmptySlots() > 0;
            pg.insertTuple(t);
            dirtyPages.add(pg);
        }
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId recId = t.getRecordId();
        if (recId == null)
            throw new DbException("Try to delete a tuple with invalid reference");
        HeapPageId pid = (HeapPageId) recId.getPageId();
        if (pid.getTableId() != getId())
            throw new DbException("Try to delete a tuple in another HeapFile");
        HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        pg.deleteTuple(t);
        ArrayList<Page> dirtyPages = new ArrayList<Page>();
        dirtyPages.add(pg);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        class HeapFileIterator implements DbFileIterator {

            private int _curPgNo;
            private int _nxtPgNo;
            private int _tableId;
            private Permissions _per = Permissions.READ_WRITE;

            private Iterator<Tuple> _curPgItr = null;

            private void redirPgItr() throws DbException, TransactionAbortedException {
                try {
                    if (_curPgItr != null)
                        Database.getBufferPool().releasePage(tid, new HeapPageId(_tableId, _curPgNo));
                    _curPgNo = _nxtPgNo;
                    _curPgItr = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(_tableId, _nxtPgNo++), _per)).iterator();
                } catch (DbException e) {
                    throw e;
                }
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                _curPgNo = -1;
                _nxtPgNo = 0;
                _tableId = getId();
                redirPgItr();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (_curPgItr == null)
                    return false;
                while (!_curPgItr.hasNext() && _nxtPgNo < _numPg)
                    redirPgItr();
                return _curPgItr.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) throw new NoSuchElementException();
                return _curPgItr.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                _nxtPgNo = 0;
                redirPgItr();
            }

            @Override
            public void close() {
                Database.getBufferPool().releasePage(tid, new HeapPageId(_tableId, _curPgNo));
                _curPgItr = null;
            }
        }

        return new HeapFileIterator();
    }

}

