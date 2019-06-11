package simpledb;

public class LockException extends Exception {
    private static final long serialVersionUID = 1L;
    private String errorMessage;

    public LockException(String m) {
        this.errorMessage = m;
    }

    public LockException() {
        this.errorMessage = null;
    }
}
