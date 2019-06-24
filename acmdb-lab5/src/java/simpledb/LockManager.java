package simpledb;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class LockManager {
    class LockStatus {
        private int s;
        private PageId pid;

        public LockStatus(PageId pid) {
            s = 0;
            this.pid = pid;
        }

        public synchronized boolean getSlock(TransactionId tid)
                throws TransactionAbortedException {
            while (true) {
                if (s >= 0) {
                    s += 1;
                    blocking.remove(tid);
                    return true;
                }
                else {
                    if (hasDeadLock(tid, pid))
                        throw new TransactionAbortedException();
                    blocking.putIfAbsent(tid, pid);
                    try {wait();} catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public synchronized boolean relSlock() {
            if (s > 0) {
                s -= 1;
                notifyAll();
                return true;
            }
            return false;
        }

        public synchronized boolean getXlock(TransactionId tid, boolean holdsSlock)
                throws TransactionAbortedException {
            while (true) {
                if (s == 0 || (s == 1 && holdsSlock)) {
                    s = -1;
                    blocking.remove(tid);
                    return true;
                }
                else {
                    if (hasDeadLock(tid, pid))
                        throw new TransactionAbortedException();
                    blocking.putIfAbsent(tid, pid);
                    try {wait();} catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public synchronized boolean relXlock() {
            if (s == -1) {
                s = 0;
                notifyAll();
                return true;
            }
            return false;
        }

        public synchronized boolean uniqueSlock() {
            return (s == 1);
        }

        public synchronized boolean updateSlock() {
            if (s == 1) {
                s = -1;
                return true;
            }
            return false;
        }


    }

    private Map<PageId, LockStatus> pid2ls;
    private Map<TransactionId, Map<PageId, Permissions>> tid2pids;
    private Map<TransactionId, PageId> blocking;
    private Map<PageId, Map<TransactionId, Permissions>> pid2tids;

    public LockManager() {
        pid2ls = new ConcurrentHashMap<PageId, LockStatus>();
        tid2pids = new ConcurrentHashMap<TransactionId, Map<PageId, Permissions>>();
        blocking = new ConcurrentHashMap<TransactionId, PageId>();
        pid2tids = new ConcurrentHashMap<PageId, Map<TransactionId, Permissions>>();
    }

    public boolean getLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {
        pid2ls.putIfAbsent(pid, new LockStatus(pid));
        tid2pids.putIfAbsent(tid, new ConcurrentHashMap<PageId, Permissions>());
        pid2tids.putIfAbsent(pid, new ConcurrentHashMap<TransactionId, Permissions>());
        LockStatus ls = pid2ls.get(pid);
        Map<PageId, Permissions> pidPerms = tid2pids.get(tid);
        Map<TransactionId, Permissions> tidPerms = pid2tids.get(pid);
        Permissions curPerm = pidPerms.get(pid);

        boolean res;
        boolean holdsLock = false;
        if (curPerm != null) {
            holdsLock = true;
            if (curPerm.permLevel >= perm.permLevel)
                return true;
            else {
                if (ls.uniqueSlock()) {
                    pidPerms.replace(pid, Permissions.READ_WRITE);
                    return ls.updateSlock();
                }
            }
        }
        if (perm == Permissions.READ_WRITE) {
            res = ls.getXlock(tid, holdsLock);
        }
        else
            res = ls.getSlock(tid);
        pidPerms.put(pid, perm);
        tidPerms.put(tid, perm);
        return res;
    }

    public boolean relLock(TransactionId tid, PageId pid) {
        pid2ls.putIfAbsent(pid, new LockStatus(pid));
        tid2pids.putIfAbsent(tid, new ConcurrentHashMap<PageId, Permissions>());
        pid2tids.putIfAbsent(pid, new ConcurrentHashMap<TransactionId, Permissions>());
        LockStatus ls = pid2ls.get(pid);
        Map<PageId, Permissions> pidPerms = tid2pids.get(tid);
        Map<TransactionId, Permissions> tidPerms = pid2tids.get(pid);
        Permissions curPerm = pidPerms.remove(pid);
        tidPerms.remove(tid);

        if (curPerm == null)
            return false;
        else if (curPerm == Permissions.READ_WRITE)
            return ls.relXlock();
        else
            return ls.relSlock();
    }

    public void relTrans(TransactionId tid) {
        tid2pids.putIfAbsent(tid, new ConcurrentHashMap<PageId, Permissions>());
        Map<PageId, Permissions> pidPerms = tid2pids.remove(tid);

        synchronized (pidPerms) {
            for (Map.Entry<PageId, Permissions> e : pidPerms.entrySet()) {
                PageId pid = e.getKey();
                pid2tids.putIfAbsent(pid, new ConcurrentHashMap<TransactionId, Permissions>());
                Map<TransactionId, Permissions> tidPerms = pid2tids.get(pid);
                tidPerms.remove(tid);
                LockStatus ls = pid2ls.get(pid);
                if (e.getValue() == Permissions.READ_WRITE)
                    ls.relXlock();
                else
                    ls.relSlock();
            }
        }

        blocking.remove(tid);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        tid2pids.putIfAbsent(tid, new ConcurrentHashMap<PageId, Permissions>());
        Map<PageId, Permissions> pidPerms = tid2pids.get(tid);
        return pidPerms.containsKey(pid);
    }

    public synchronized boolean hasDeadLock(TransactionId tid, PageId pid) {
        Stack<TransactionId> ts = new Stack<TransactionId>();
        Set<TransactionId> visit = new HashSet<TransactionId>();

        visit.add(tid);
        for (Map.Entry<TransactionId, Permissions> e : pid2tids.get(pid).entrySet()) {
            TransactionId nt = e.getKey();
            if (!nt.equals(tid))
                ts.push(nt);
        }
        while (!ts.empty()) {
            TransactionId ct = ts.peek();
            if (visit.contains(ct)) {
                ts.pop();
                visit.remove(ct);
            }
            visit.add(ct);
            PageId waitPg = blocking.get(ct);
            if (waitPg == null)
                continue;
            for (Map.Entry<TransactionId, Permissions> e : pid2tids.get(waitPg).entrySet()) {
                TransactionId nt = e.getKey();
                if (visit.contains(nt))
                    return true;
                else
                    ts.push(nt);
            }
        }
        return false;
    }

    public Set<PageId> getWrittenPage(TransactionId tid) {
        tid2pids.putIfAbsent(tid, new ConcurrentHashMap<PageId, Permissions>());
        Map<PageId, Permissions> pidPerms = tid2pids.get(tid);
        Set<PageId> writtenPages = new HashSet<PageId>();
        synchronized (pidPerms) {
            for (Map.Entry<PageId, Permissions> e : pidPerms.entrySet()) {
                if (e.getValue() == Permissions.READ_WRITE)
                    writtenPages.add(e.getKey());
            }
        }
        return writtenPages;
    }

}