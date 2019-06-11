package simpledb;

public class Lock {
    private int slock;
    private int xlock;

    public Lock() {
        slock = 0;
        xlock = 0;
    }

    public boolean getSlock() {
        synchronized (this) {
            if (xlock == 0) {
                slock += 1;
                return true;
            }
            return false;
        }
    }

    public boolean releaseSlock() {
        synchronized (this) {
            if (slock == 0)
                return false;
            slock -= 1;
            return true;
        }
    }

    public boolean getXlock() {
        synchronized (this) {
            if (xlock == 0 && slock == 0) {
                xlock += 1;
                return true;
            }
            return false;
        }
    }

    public boolean upgradeLock() {
        synchronized (this) {
            if (xlock == 0 && slock == 1) {
                xlock = 1;
                slock = 0;
                return true;
            }
            return false;
        }
    }

    public boolean releaseXlock() {
        synchronized (this) {
            if (xlock == 0)
                return false;
            xlock -= 1;
            return true;
        }
    }

}
