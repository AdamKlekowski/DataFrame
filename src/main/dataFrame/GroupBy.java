package main.dataFrame;

public interface GroupBy {
    DataFrame max() throws InterruptedException;
    DataFrame min() throws InterruptedException;

    DataFrame mean() throws ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException;
    DataFrame std();
    DataFrame sum() throws ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException;
    DataFrame var();
}
