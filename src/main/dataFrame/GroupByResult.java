package main.dataFrame;

import main.client.EchoClient;
import main.value.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GroupByResult implements GroupBy {
    private HashMap<ArrayList<Value>,DataFrame> groupedDataFrame;
    private DataFrame resultDataFrame;
    private ArrayList<String> avoidedCol;

    GroupByResult(HashMap<ArrayList<Value>, DataFrame> newValue, DataFrame newDF, ArrayList<String> avoidedCol) {
        groupedDataFrame = newValue;
        resultDataFrame = newDF;
        this.avoidedCol = avoidedCol;
    }

    private void removeRedundantCol() {
        for (int i = 0; i < resultDataFrame.columns.size(); i++) {
            if (! (resultDataFrame.columns.get(i).type.equals("IntegerValue") ||
                    resultDataFrame.columns.get(i).type.equals("DoubleValue") ||
                    avoidedCol.contains(resultDataFrame.columns.get(i).name))) {
                resultDataFrame.columns.remove(i);
                removeRedundantCol();
                break;
            }
        }
    }

    @Override
    public DataFrame max() throws InterruptedException {
        //ArrayList<Thread> threads = new ArrayList<>();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

        for (DataFrame df : groupedDataFrame.values()) {
            final DataFrame finalDF = df;

            Thread t = new Thread(() -> {
                Value[] rawValue = new Value[resultDataFrame.columns.size()];
                int size = finalDF.columns.size();
                for (int i = 0; i < size; i++) {
                    if (avoidedCol.contains(finalDF.columns.get(i).name)) {
                        rawValue[i] = df.columns.get(i).records.get(0);
                        continue;
                    }
                    rawValue[i] = finalDF.columns.get(i).records.get(0);
                    for (Value rawElement : finalDF.columns.get(i).records) {
                        if (rawElement.gte(rawValue[i])) rawValue[i] = rawElement;
                    }
                }
                resultDataFrame.add(rawValue);
            });
            executor.execute(t);
            //threads.add(t);
        }
/*        TimeUnit.SECONDS.sleep(1);
        executor.shutdown();*/

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {}

/*        for (Thread t : threads) {
            t.join();
        }
        threads.clear();*/
        return resultDataFrame;
    }

    @Override
    public DataFrame min() throws InterruptedException {
        //ArrayList<Thread> threads = new ArrayList<>();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

        for (DataFrame df : groupedDataFrame.values()) {
            final DataFrame finalDF = df;
            Thread t = new Thread(() -> {
                Value[] rawValue = new Value[resultDataFrame.columns.size()];
                int size = finalDF.columns.size();
                for (int i = 0; i < size; i++) {
                    if (avoidedCol.contains(finalDF.columns.get(i).name)) {
                        rawValue[i] = df.columns.get(i).records.get(0);
                        continue;
                    }
                    rawValue[i] = finalDF.columns.get(i).records.get(0);
                    for (Value rawElement : finalDF.columns.get(i).records) {
                        if (rawElement.lte(rawValue[i])) rawValue[i] = rawElement;
                    }
                }
                resultDataFrame.add(rawValue);
            });
/*            t.start();
            threads.add(t);*/
            executor.execute(t);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {}
/*        for (Thread t : threads) {
            t.join();
        }
        threads.clear();*/
        return resultDataFrame;
    }

    @Override
    public DataFrame mean() throws InterruptedException {
        removeRedundantCol();
        //ArrayList<Thread> threads = new ArrayList<>();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

        for (DataFrame df : groupedDataFrame.values()) {
            final DataFrame finalDF = df;
            Thread t = new Thread(() -> {
                Value[] rawValue = new Value[resultDataFrame.columns.size()];
                Class c = null;
                DoubleValue sum = null;
                try {
                    c = Class.forName("main.value.DoubleValue");
                    sum = ((DoubleValue) c.newInstance()).create("0");
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                }

                int j = 0;
                boolean isShouldAdd = false;
                int size = finalDF.columns.size();
                for (int i = 0; i < size; i++) {
                    if (avoidedCol.contains(finalDF.columns.get(i).name)) {
                        rawValue[j] = finalDF.columns.get(i).records.get(0);
                        j++;
                        continue;
                    }

                    if (finalDF.columns.get(i).type.equals("IntegerValue") || finalDF.columns.get(i).type.equals("DoubleValue")) {
                        for (Value rawElement : finalDF.columns.get(i).records) {
                            sum = sum.add(rawElement);
                        }
                        isShouldAdd = true;
                    }

                    if(isShouldAdd) {
                        DoubleValue numberOfElement = null;
                        try {
                            numberOfElement = ((DoubleValue) c.newInstance()).create(String.valueOf(df.size()));
                        } catch (InstantiationException | IllegalAccessException e) {
                            e.printStackTrace();
                        }
                        rawValue[j] = sum.div(numberOfElement);
                        j++;
                        try {
                            sum = ((DoubleValue) c.newInstance()).create("0");
                        } catch (InstantiationException | IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                }
                resultDataFrame.add(rawValue);
            });
            executor.execute(t);
           /* t.start();
            threads.add(t);*/
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {}

/*        for (Thread t : threads) {
            t.join();
        }
        threads.clear();*/
        return resultDataFrame;
    }

    @Override
    public DataFrame std() {
        removeRedundantCol();
        return null;
    }

    @Override
    public DataFrame sum() throws InterruptedException {
        removeRedundantCol();
        //ArrayList<Thread> threads = new ArrayList<>();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

        for (DataFrame df : groupedDataFrame.values()) {
            final DataFrame finalDF = df;
            Thread t = new Thread(() -> {
                Value[] rawValue = new Value[resultDataFrame.columns.size()];
                Class c = null;
                DoubleValue sum = null;
                try {
                    c = Class.forName("main.value.DoubleValue");
                    sum = ((DoubleValue) c.newInstance()).create("0");
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                }

                int j=0;
                boolean isShouldAdd = false;
                int size = finalDF.columns.size();
                for (int i = 0; i < size; i++) {
                    if (avoidedCol.contains(finalDF.columns.get(i).name)) {
                        rawValue[j] = finalDF.columns.get(i).records.get(0);
                        j++;
                        continue;
                    }

                    if (finalDF.columns.get(i).type.equals("IntegerValue") || finalDF.columns.get(i).type.equals("DoubleValue")) {
                        for (Value rawElement : finalDF.columns.get(i).records) {
                            sum = sum.add(rawElement);
                        }
                        isShouldAdd=true;
                    }

                    if(isShouldAdd) {
                        rawValue[j] = sum;
                        j++;
                        try {
                            sum = ((DoubleValue) c.newInstance()).create("0");
                        } catch (InstantiationException | IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                }
                resultDataFrame.add(rawValue);
            });
            executor.execute(t);
/*            t.start();
            threads.add(t);*/
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {}

/*        for (Thread t : threads) {
            t.join();
        }
        threads.clear();*/
        return resultDataFrame;
    }

    @Override
    public DataFrame var() {
        removeRedundantCol();
        return null;
    }

    public DataFrame maxWithoutMultiThreading() {
        Value[] rawValue = new Value[resultDataFrame.columns.size()];

        for (DataFrame df : groupedDataFrame.values()) {
            for (int i = 0; i < df.columns.size(); i++) {

                if (avoidedCol.contains(df.columns.get(i).name)) {
                    rawValue[i] = df.columns.get(i).records.get(0);
                    continue;
                }

                rawValue[i] = df.columns.get(i).records.get(0);
                for (Value rawElement : df.columns.get(i).records) {
                    if (rawElement.gte(rawValue[i])) rawValue[i] = rawElement;
                }
            }
            resultDataFrame.add(rawValue);
        }
        return resultDataFrame;
    }

    public DataFrame minWithoutMultiThreading() {
        Value[] rawValue = new Value[resultDataFrame.columns.size()];

        for (DataFrame df : groupedDataFrame.values()) {
            for (int i = 0; i < df.columns.size(); i++) {

                if (avoidedCol.contains(df.columns.get(i).name)) {
                    rawValue[i] = df.columns.get(i).records.get(0);
                    continue;
                }

                rawValue[i] = df.columns.get(i).records.get(0);
                for (Value rawElement : df.columns.get(i).records) {
                    if (rawElement.lte(rawValue[i])) rawValue[i] = rawElement;
                }
            }
            resultDataFrame.add(rawValue);
        }
        return resultDataFrame;
    }

    public DataFrame meanWithoutMultiThreading() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        removeRedundantCol();

        Value[] rawValue = new Value[resultDataFrame.columns.size()];
        Class c = Class.forName("main.value.DoubleValue");
        DoubleValue sum = ((DoubleValue) c.newInstance()).create("0");

        for (DataFrame df : groupedDataFrame.values()) {
            int j=0;
            boolean isShouldAdd = false;
            for (int i = 0; i < df.columns.size(); i++) {
                if (avoidedCol.contains(df.columns.get(i).name)) {
                    rawValue[j] = df.columns.get(i).records.get(0);
                    j++;
                    continue;
                }

                if (df.columns.get(i).type.equals("IntegerValue") || df.columns.get(i).type.equals("DoubleValue")) {
                    for (Value rawElement : df.columns.get(i).records) {
                        sum = sum.add(rawElement);
                    }
                    isShouldAdd=true;
                }

                if(isShouldAdd) {
                    DoubleValue numberOfElement = ((DoubleValue) c.newInstance()).create(String.valueOf(df.size()));
                    rawValue[j] = sum.div(numberOfElement);
                    j++;
                    sum = ((DoubleValue) c.newInstance()).create("0");
                }
            }
            resultDataFrame.add(rawValue);
        }
        return resultDataFrame;
    }

    public DataFrame stdWithoutMultiThreading() {
        removeRedundantCol();
        return null;
    }

    public DataFrame sumWithoutMultiThreading() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        removeRedundantCol();

        Value[] rawValue = new Value[resultDataFrame.columns.size()];
        Class c = Class.forName("main.value.DoubleValue");
        DoubleValue sum = ((DoubleValue) c.newInstance()).create("0");
        for (DataFrame df : groupedDataFrame.values()) {
            int j = 0;
            boolean isShouldAdd = false;
            for (int i = 0; i < df.columns.size(); i++) {
                if (avoidedCol.contains(df.columns.get(i).name)) {
                    rawValue[j] = df.columns.get(i).records.get(0);
                    j++;
                    continue;
                }

                if (df.columns.get(i).type.equals("IntegerValue") || df.columns.get(i).type.equals("DoubleValue")) {
                    for (Value rawElement : df.columns.get(i).records) {
                        sum = sum.add(rawElement);
                    }
                    isShouldAdd = true;
                }

                if (isShouldAdd) {
                    rawValue[j] = sum;
                    j++;
                    sum = ((DoubleValue) c.newInstance()).create("0");
                }
            }
            resultDataFrame.add(rawValue);
        }
        return resultDataFrame;
    }

    public DataFrame varWithoutMultiThreading() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        removeRedundantCol();

        Value[] rawValue = new Value[resultDataFrame.columns.size()];
        Class c = Class.forName("main.value.DoubleValue");
        DoubleValue sum = ((DoubleValue) c.newInstance()).create("0");

        for (DataFrame df : groupedDataFrame.values()) {
            int j=0;
            boolean isShouldAdd = false;
            for (int i = 0; i < df.columns.size(); i++) {
                if (avoidedCol.contains(df.columns.get(i).name)) {
                    rawValue[j] = df.columns.get(i).records.get(0);
                    j++;
                    continue;
                }

                if (df.columns.get(i).type.equals("IntegerValue") || df.columns.get(i).type.equals("DoubleValue")) {
                    for (Value rawElement : df.columns.get(i).records) {
                        sum = sum.add(rawElement);
                    }
                    isShouldAdd=true;
                }

                if(isShouldAdd) {
                    DoubleValue numberOfElement = ((DoubleValue) c.newInstance()).create(String.valueOf(df.size()));
                    rawValue[j] = sum.div(numberOfElement);
                    j++;
                    sum = ((DoubleValue) c.newInstance()).create("0");
                }
            }
            resultDataFrame.add(rawValue);
        }
        return resultDataFrame;
    }

    public DataFrame maxInCluster() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        for (DataFrame df : groupedDataFrame.values()) {
            DataFrame partOfDf = main.client.EchoClient.calculate(df, "max");
            resultDataFrame.addDf(partOfDf);
        }
        return resultDataFrame;
    }

    public DataFrame minInCluster() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        for (DataFrame df : groupedDataFrame.values()) {
            DataFrame partOfDf = main.client.EchoClient.calculate(df, "min");
            resultDataFrame.addDf(partOfDf);
        }
        return resultDataFrame;
    }
}