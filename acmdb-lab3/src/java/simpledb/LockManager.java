package simpledb;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager
{
    public class Locker
    {
        private TransactionId  tid;
        private ConcurrentHashMap<PageId, Permissions> lockedPages;

        public Locker(TransactionId tid) {
            this.tid = tid;
            lockedPages = new ConcurrentHashMap<PageId, Permissions>();
        }

        public boolean holdsLock(PageId pid) {
            return (lockedPages.get(pid) != null);
        }
        public synchronized boolean lock(PageId pid, Permissions perm) {
            Permissions curPer = lockedPages.get(pid);
            if (curPer != null) { // The transaction have had lock the page
                if (curPer.permLevel >= perm.permLevel)
                    return true;
                else {
                    lockedPages.remove(pid);
                    removeReadingLock(pid, this);
                    addWritingLock(pid, this);
                    lockedPages.put(pid, perm);
                    return true;
                }
            }
            else {
                boolean lockResult;
                if (perm == Permissions.READ_ONLY)
                    lockResult = addReadingLock(pid, this);
                else
                    lockResult = addWritingLock(pid, this);

                if (lockResult)
                    lockedPages.put(pid, perm);
                return lockResult;
            }
        }

        public synchronized void release(PageId pid) {
            Permissions curPer = lockedPages.get(pid);
            if (curPer != null) {
                if (curPer == Permissions.READ_WRITE)
                    removeWritingLock(pid);
                else
                    removeReadingLock(pid, this);
                lockedPages.remove(pid);
            }
        }

        public synchronized void clear() {
            for (Map.Entry<PageId, Permissions> e : lockedPages.entrySet()) {
                if (e.getValue() == Permissions.READ_WRITE)
                    removeWritingLock(e.getKey());
                else
                    removeReadingLock(e.getKey(), this);
            }
            lockedPages.clear();
        }

    }

    private ConcurrentHashMap<TransactionId, Locker> registedLockers;
    private ConcurrentHashMap<PageId, Locker> writeLockers;
    private ConcurrentHashMap<PageId, LinkedList<Locker>> readLockers;

    public LockManager() {
        registedLockers = new ConcurrentHashMap<TransactionId, Locker>();
        writeLockers = new ConcurrentHashMap<PageId, Locker>();
        readLockers = new ConcurrentHashMap<PageId, LinkedList<Locker>>();
    }

    public Locker getLocker(TransactionId tid) {
        Locker locker = registedLockers.get(tid);
        if (locker == null) {
            locker = new Locker(tid);
            registedLockers.put(tid, new Locker(tid));
        }
        return locker;
    }

    public void removeLocker(TransactionId tid) {
        Locker locker = registedLockers.get(tid);
        if (locker != null)
            locker.clear();
    }

    public boolean addWritingLock(PageId pid, Locker locker) {
        Locker writeLockHolder = writeLockers.get(pid);
        if (writeLockHolder == null) {
            LinkedList<Locker> rll = readLockers.get(pid);
            if (rll != null)
                return false;
            else
                writeLockers.put(pid, locker);
            return true;
        }
        else
            return writeLockHolder == locker;
    }

    public void removeWritingLock(PageId pid) {
        writeLockers.remove(pid);
    }

    public boolean addReadingLock(PageId pid, Locker locker) {
        Locker writeLockHolder = writeLockers.get(pid);
        if (writeLockHolder == null) {
            LinkedList<Locker> lockers = readLockers.get(pid);
            if (lockers == null) {
                lockers = new LinkedList<Locker>();
                readLockers.put(pid, lockers);
            }
            lockers.add(locker);
            return true;
        }
        return false;
    }

    public void removeReadingLock(PageId pid, Locker locker) {
        LinkedList<Locker> list = readLockers.get(pid);
        if (list != null) {
            list.remove(locker);
            if (list.peekFirst() == null)
                readLockers.remove(pid);
        }
    }
}
