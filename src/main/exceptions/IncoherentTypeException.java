package main.exceptions;

public class IncoherentTypeException extends Exception {
    private String colName;
    private int rawNum;

    public IncoherentTypeException(String colName, int rawNum) {
        this.colName = colName;
        this.rawNum = rawNum;
    }

    public void print() {
        System.out.println("Incoherent type in column \"" + colName + "\", raw number: " + rawNum);
    }
}
