package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

import static simpledb.Permissions.READ_ONLY;
import static simpledb.Permissions.READ_WRITE;

public class LockManager {

    class LockStatus {
        public PageId pid;
        public Permissions per;

        public LockStatus(PageId pid, Permissions per) {
            this.pid = pid;
            this.per = per;
        }

        public boolean equals(Object ls) {
            if (this == ls)
                return true;
            if (!(ls instanceof LockStatus))
                return false;
            LockStatus lss = (LockStatus) ls;
            return this.pid.equals(lss.pid) && this.per == lss.per;
        }
    }

    class OrderedLock {
        public TransactionId tid;
        public Permissions per;
        public Thread t;

        public OrderedLock(TransactionId tid, Permissions per, Thread t) {
            this.tid = tid;
            this.per = per;
            this.t = t;
        }

        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof OrderedLock))
                return false;
            OrderedLock oo = (OrderedLock) o;
            return this.tid.equals(oo.tid) && this.per == oo.per;
        }
    }


    private Map<PageId, Lock> locks;
    private Map<TransactionId, List<LockStatus>> tranOccu;
    private Map<PageId, Set<TransactionId>> lockRegister;
    private Map<PageId, List<OrderedLock>> waitQueues;
    private Map<TransactionId, PageId> blocks;

    public LockManager() {
        locks = new ConcurrentHashMap<PageId, Lock>();
        tranOccu = new ConcurrentHashMap<TransactionId, List<LockStatus>>();
        lockRegister = new ConcurrentHashMap<PageId, Set<TransactionId>>();
        waitQueues = new ConcurrentHashMap<PageId, List<OrderedLock>>();
        blocks = new ConcurrentHashMap<TransactionId, PageId>();
    }

    private void signInWait(TransactionId tid, PageId pid,
                            Permissions per, Thread t)
            throws TransactionAbortedException {
        if (hasDeadlock(tid, pid))
            throw new TransactionAbortedException();
        blocks.putIfAbsent(tid, pid);
        waitQueues.putIfAbsent(pid, new LinkedList<OrderedLock>());
        List<OrderedLock> queue = waitQueues.get(pid);
        synchronized (queue) {
            OrderedLock olock = new OrderedLock(tid, per, t);
            if (!queue.contains(olock))
                queue.add(olock);
        }
    }

    private void signOutWait(TransactionId tid, PageId pid)
                            throws TransactionAbortedException {
        blocks.remove(tid);
        List<OrderedLock> queue = waitQueues.get(pid);
        if (queue == null)
            return;
        synchronized (queue) {
            queue.remove(tid);
        }
    }

    private void step(PageId pid) {
        List<OrderedLock> queue = waitQueues.get(pid);
        OrderedLock o;
        if (queue == null)
            return;
        synchronized (queue) {
            if (queue.size() == 0)
                return;
            o = queue.remove(0);
        }
        blocks.remove(o.tid);
        LockSupport.unpark(o.t);
    }

    public boolean holdsLock(TransactionId tid, PageId pid, Permissions per) {
        tranOccu.putIfAbsent(tid, new LinkedList<LockStatus>());
        List<LockStatus> occu = tranOccu.get(tid);
        LockStatus status = new LockStatus(pid, per);
        synchronized (occu) {
            return occu.contains(status);
        }
    }

    public boolean getLock(TransactionId tid, PageId pid, Permissions per)
            throws TransactionAbortedException {
        if (per == READ_ONLY)
            return getSlock(tid, pid);
        else
            return getXlock(tid, pid);
    }

    public boolean getSlock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        LockStatus ls = new LockStatus(pid, READ_ONLY);
        tranOccu.putIfAbsent(tid, new LinkedList<LockStatus>());
        List<LockStatus> occu = tranOccu.get(tid);
        lockRegister.putIfAbsent(pid, new HashSet<TransactionId>());
        Set<TransactionId> holders = lockRegister.get(pid);
        synchronized (occu) {
            if (occu.contains(new LockStatus(pid, READ_WRITE)))
                return true;
            if (occu.contains(ls))
                return true;
        }

        locks.putIfAbsent(pid, new Lock());
        Lock lock = locks.get(pid);
        if (!lock.getSlock()) {
            Thread t = Thread.currentThread();
            signInWait(tid, pid, READ_ONLY, t);
            while (true) {
                LockSupport.park();
                if (lock.getSlock())
                    break;
            }
        }
        synchronized (occu) {
            occu.add(ls);
        }
        synchronized (holders) {
            holders.add(tid);
        }

        return true;
    }

    public boolean getXlock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        LockStatus lsr = new LockStatus(pid, READ_ONLY);
        LockStatus lsw = new LockStatus(pid, READ_WRITE);
        locks.putIfAbsent(pid, new Lock());
        Lock lock = locks.get(pid);
        tranOccu.putIfAbsent(tid, new LinkedList<LockStatus>());
        List<LockStatus> occu = tranOccu.get(tid);
        lockRegister.putIfAbsent(pid, new HashSet<TransactionId>());
        Set<TransactionId> holders = lockRegister.get(pid);
        synchronized (occu) {
            if (occu.contains(lsw))
                return true;
            if (occu.contains(lsr) && lock.upgradeLock()) {
                occu.remove(lsr);
                occu.add(lsw);
                return true;
            }
        }


        if (!lock.getXlock()) {
            Thread t = Thread.currentThread();
            signInWait(tid, pid, READ_WRITE, t);
            while (true) {
                LockSupport.park();
                synchronized (occu) {
                    if (occu.contains(lsr) && lock.upgradeLock()) {
                        occu.remove(lsr);
                        occu.add(lsw);
                        return true;
                    }
                }
                if (lock.getXlock())
                    break;
            }
        }

        synchronized (occu) {
            occu.add(lsw);
        }
        synchronized (holders) {
            holders.add(tid);
        }
        return true;
    }


    public boolean releaseLock(TransactionId tid, PageId pid, Permissions per) {
        LockStatus ls = new LockStatus(pid, per);

        tranOccu.putIfAbsent(tid, new LinkedList<LockStatus>());
        List<LockStatus> occu = tranOccu.get(tid);
        synchronized (occu) {
            if (!occu.remove(ls))
                return false;
        }
        lockRegister.putIfAbsent(pid, new HashSet<TransactionId>());
        Set<TransactionId> holders = lockRegister.get(pid);
        synchronized (holders) {
            holders.remove(tid);
        }

        locks.putIfAbsent(pid, new Lock());
        boolean res = per.permLevel == 0? locks.get(pid).releaseSlock() :
                locks.get(pid).releaseXlock();
        if (res) {
            step(pid);
        }
        return res;
    }


    public void releaseOccu(TransactionId tid) {
        List<LockStatus> occu = tranOccu.remove(tid);
        if (occu == null)
            return;
        synchronized (occu) {
            Iterator<LockStatus> itr = occu.iterator();
            while (itr.hasNext()) {
                LockStatus ls = itr.next();
                itr.remove();
                lockRegister.putIfAbsent(ls.pid, new HashSet<TransactionId>());
                Set<TransactionId> holders = lockRegister.get(ls.pid);
                synchronized (holders) {
                    holders.remove(tid);
                }
                locks.putIfAbsent(ls.pid, new Lock());
                boolean res = ls.per.permLevel == 0 ? locks.get(ls.pid).releaseSlock() :
                        locks.get(ls.pid).releaseXlock();
                if (res) {
                    step(ls.pid);
                }
            }
        }
    }

    public boolean hasDeadlock(TransactionId tid, PageId pid) {
        Set<TransactionId> holders;
        Stack<TransactionId> stack = new Stack<TransactionId>();
        Set<TransactionId> visit = new HashSet<TransactionId>();

        holders = lockRegister.get(pid);
        synchronized (holders) {
            if (holders != null)
                stack.addAll(holders);
        }
        while (!stack.empty()) {
            TransactionId nowt = stack.peek();
            if (visit.contains(nowt)) {
                stack.pop();
                visit.remove(nowt);
                continue;
            }
            visit.add(nowt);
            PageId nxtPid = blocks.get(nowt);
            if (nxtPid == null)
                continue;
            holders = lockRegister.get(nxtPid);
            synchronized (holders) {
                Iterator<TransactionId> itr = holders.iterator();
                while (itr.hasNext()) {
                    TransactionId nxtt = itr.next();
                    if (visit.contains(nxtt))
                        return true;
                    else
                        stack.add(nxtt);
                }
            }
        }
        return false;
    }

}