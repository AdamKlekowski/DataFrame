package main.dataFrame;

import main.value.*;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import java.lang.IllegalArgumentException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataFrame {
    public ArrayList<Column> columns;

    public DataFrame() {
        columns = new ArrayList<>();
    }

    public DataFrame(String[] columnNames, String[] columnTypes) {
        if(columnNames.length != columnTypes.length){
            throw new IllegalArgumentException("Number of columnNames and columnTypes are not equal.");
        }

        columns = new ArrayList<>();

        for (int i=0; i<columnNames.length; i++) {
            columns.add(new Column(columnNames[i], columnTypes[i]));
        }
    }

    public String serialDataFrame() {
        StringBuilder serializedDataFrame = new StringBuilder();
        boolean isFirst = true;
        for (Column col : columns) {
            if(!isFirst) {
                serializedDataFrame.append("<>");
            }
            serializedDataFrame.append(col.name).append(";");
            serializedDataFrame.append(col.type);
            for (Value val : col.records) {
                serializedDataFrame.append(";").append(val.toString());
            }

            isFirst = false;
        }
        return serializedDataFrame.toString();
    }

    void addDf(DataFrame dfToAdd) {
        for (int i=0; i<dfToAdd.columns.size(); i++) {
            for(Value val : dfToAdd.columns.get(i).records) {
                columns.get(i).records.add(val);
            }
        }
    }

    public static DataFrame deserialDataFrame(String serializedDataFrame) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        DataFrame result = new DataFrame();
        String[] columnsFromString = serializedDataFrame.split("<>");
        for (String col : columnsFromString) {
            String[] splitedColumns = col.split(";");

            Column newCol = new Column(splitedColumns[0], splitedColumns[1]);
            for (int i=2; i<splitedColumns.length; i++) {
                Value newVal;

                Class c = Class.forName("main.value." + splitedColumns[1]);
                switch (splitedColumns[1]) {
                    case "StringValue":
                        newVal = ((StringValue) c.newInstance()).create(splitedColumns[i]);
                        break;
                    case "DoubleValue":
                        newVal = ((DoubleValue) c.newInstance()).create(splitedColumns[i]);
                        break;
                    case "IntegerValue":
                        newVal = ((IntegerValue) c.newInstance()).create(splitedColumns[i]);
                        break;
                    case "DataTimeValue":
                        newVal = ((DataTimeValue) c.newInstance()).create(splitedColumns[i]);
                        break;
                    default:
                        throw new IllegalArgumentException(splitedColumns[1] + " was not recognised as a date frame type");
                }

                newCol.records.add(newVal);
            }


            result.columns.add(newCol);
        }
        return result;
    }

    /**
     * Constructor using to load data from file
     * @param fileName - name of file with input data
     * @param columnTypes - types of columns (if "null" try recognised using regex matches)
     * @param columnNames - names of columns (if "null" read names from first line of file)
     * @throws IOException - exception if there are problems with open or read from file
     */
    public DataFrame(String fileName, String[] columnTypes, String[] columnNames) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        FileInputStream fileStream = new FileInputStream(fileName);
        BufferedReader br = new BufferedReader(new InputStreamReader(fileStream));

        String strLine;
        String splited;
        String[] splitedArray;

        if(columnTypes == null) {
            strLine = br.readLine();
            strLine = br.readLine();

            splited = strLine;
            splitedArray = splited.split(",");
            columnTypes = new String[splitedArray.length];

            Pattern stringPattern = Pattern.compile("^[A-Za-z]*$");
            Pattern integerPattern = Pattern.compile("^[1-9][0-9]*$");
            Pattern doublePattern = Pattern.compile("^(-)?[0-9]*.[0-9]*$");
            Pattern datePattern = Pattern.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$");
            for (int i=0; i<splitedArray.length; i++) {
                Matcher stringMatcher = stringPattern.matcher(splitedArray[i]);
                Matcher integerMatcher = integerPattern.matcher(splitedArray[i]);
                Matcher doubleMatcher = doublePattern.matcher(splitedArray[i]);
                Matcher dateMatcher = datePattern.matcher(splitedArray[i]);

                if (stringMatcher.matches()){
                    columnTypes[i] = "StringValue";
                }
                else if (integerMatcher.matches()){
                    columnTypes[i] = "IntegerValue";
                }
                else if (doubleMatcher.matches()){
                    columnTypes[i] = "DoubleValue";
                }
                else if (dateMatcher.matches()){
                    columnTypes[i] = "StringValue"; //TODO
                }
                else {
                    throw new IllegalArgumentException(splitedArray[i] + " is in not recognisable type");
                }
            }
            fileStream.close();
            fileStream = new FileInputStream(fileName);
            br = new BufferedReader(new InputStreamReader(fileStream));
        }

        if(columnNames != null){
            columns = new ArrayList<>();
            for (int i=0; i<columnNames.length; i++) {
                columns.add(new Column(columnNames[i], columnTypes[i]));
            }
        }

        if(columnNames == null){
            strLine = br.readLine();
            splited = strLine;
            splitedArray = splited.split(",");

            columns = new ArrayList<>();
            for (int i=0; i<splitedArray.length; i++) {
                columns.add(new Column(splitedArray[i], columnTypes[i]));
            }
        }

        while ((strLine = br.readLine()) != null)   {
            splited = strLine;
            splitedArray = splited.split(",");

            Value[] newRaw = new Value[splitedArray.length];

            for (int i=0; i<splitedArray.length; i++) {
                Class c = Class.forName("main.value." + columnTypes[i]);
                switch (columnTypes[i]) {
                    case "StringValue":
                        newRaw[i] = ((StringValue) c.newInstance()).create(splitedArray[i]);
                        break;
                    case "DoubleValue":
                        newRaw[i] = ((DoubleValue) c.newInstance()).create(splitedArray[i]);
                        break;
                    case "IntegerValue":
                        newRaw[i] = ((IntegerValue) c.newInstance()).create(splitedArray[i]);
                        break;
                    case "DataTimeValue":
                        newRaw[i] = ((DataTimeValue) c.newInstance()).create(splitedArray[i]);
                        break;
                    default:
                        throw new IllegalArgumentException(columnTypes[i] + " was not recognised as a date frame type");
                }
            }
            add(newRaw);
        }
        br.close();
    }

    public synchronized void add(Value[] args) {
        int columnsSize = columns.size();
        if(args.length > columnsSize){
            throw new IllegalArgumentException("Too many arguments");
        }
        else if(args.length < columnsSize){
            throw new IllegalArgumentException("Too little arguments");
        }

        for (int i=0; i<args.length; i++) {
            columns.get(i).records.add(args[i]);
        }
    }

    /**
     *
     * @return size of DataFrame - number of raws
     */
    public int size(){
        return columns.get(0).records.size();
    }

    /**
     *
     * @param colName - name of searching column
     * @return found Column
     */
    private Column get(String colName){
        for(Column col : columns){
            if(col.name.equals(colName)){
                return col;
            }
        }
        throw new IllegalArgumentException("Column not found");
    }

    /**
     * Method returning DataFrame with chosen columns
     * @param columnToCopy names of columns to copy
     * @param isDeepCopy if true, method returns deep copy
     * @return the DataFrame object with chosen columns
     * @throws CloneNotSupportedException thrown if cloning object does not implement Cloneable interface
     */
    private DataFrame get(String[] columnToCopy, boolean isDeepCopy) throws CloneNotSupportedException{
        DataFrame df = new DataFrame();
        df.columns = new ArrayList<>();
        if(isDeepCopy) {
            for(String colName : columnToCopy) {
                df.columns.add(get(colName).clone());
            }
        }
        else {
            for(String colName : columnToCopy) {
                df.columns.add(get(colName));
            }
        }
        return df;
    }

    private DataFrame iloc(int i) {
        if(i<0 || i>size()){
            throw new IndexOutOfBoundsException(i + " is out of bound (0," + size() + ")");
        }

        Value[] chosenLine = new Value[columns.size()];
        String[] columnNames = new String[columns.size()];
        String[] columnTypes = new String[columns.size()];

        for (int j=0; j<columns.size(); j++) {
            chosenLine[j]=columns.get(j).records.get(i);
            columnNames[j]=columns.get(j).name;
            columnTypes[j]=columns.get(j).type;
        }

        DataFrame newDF = new DataFrame(columnNames, columnTypes);
        newDF.add(chosenLine);
        return newDF;
    }

    public DataFrame iloc(int from, int to){
        if(from<0 || from>size()){
            throw new IndexOutOfBoundsException(from + " is out of bound");
        }
        else if (to<0 || to>size()){
            throw new IndexOutOfBoundsException(to + " is out of bound");
        }

        Value[] chosenLine = new Value[columns.size()];
        String[] columnNames = new String[columns.size()];
        String[] columnTypes = new String[columns.size()];

        for (int j=0; j<columns.size(); j++) {
            columnNames[j]=columns.get(j).name;
            columnTypes[j]=columns.get(j).type;
        }

        DataFrame newDF = new DataFrame(columnNames, columnTypes);
        for (int i=from; i<=to; i++) {
            for (int j=0; j<columns.size(); j++) {
                chosenLine[j]=columns.get(j).records.get(i);
            }
            newDF.add(chosenLine);
        }
        return newDF;
    }

    /**
     * Printing data frame names and columns.
     */
    public void print(int limit) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        String line = "------" + "+--------------------------".repeat(Math.max(0, columns.size())) + "-";
        System.out.println(line);
        System.out.printf("|%4s |", "#");
        for (Column col : columns){
            System.out.printf("%25s |", col.name);
        }
        System.out.println();
        System.out.println(line);

        if (limit == 0) limit = size();

        for (int i=0; i<limit; i++) {
            System.out.printf("|%4s |", i);
            for (Column col : columns){
                System.out.printf("%25s |", col.records.get(i));
            }
            System.out.println();
        }
        System.out.println(line);
    }

    public void setElement(int x, int y, Value newElement) {
        columns.get(y).records.set(x, newElement);
    }

    public Value getElement(int x, int y) {
        return columns.get(y).records.get(x);
    }

    /**
     *
     * @param colNames
     * @return
     */
    public GroupByResult groupBy(String[] colNames) {
        HashMap<ArrayList<Value>,DataFrame> resultMap = new HashMap<>();

        Value[] fullRaw = new Value[columns.size()];
        ArrayList<Value> keyRaw = new ArrayList<>();

        int columnsSize, size = size();
        for (int i = 0; i < size; i++) {
            keyRaw.clear();
            columnsSize=columns.size();
            for (int k = 0, j = 0; k < columnsSize; k++) {
                fullRaw[k] = columns.get(k).records.get(i);
                if (j < colNames.length && columns.get(k).name.equals(colNames[j])) {
                    keyRaw.add(columns.get(k).records.get(i));
                    j++;
                }
            }

            if (!resultMap.containsKey(keyRaw)) {
                DataFrame df = iloc(i);
                resultMap.put(keyRaw,df);
            }
            else {
                resultMap.get(keyRaw).add(fullRaw);
            }
        }

        columnsSize = columns.size();
        String[] colName = new String[columnsSize];
        String[] colType = new String[columnsSize];
        for (int i = 0; i < columnsSize; i++) {
            colName[i] = columns.get(i).name;
            colType[i] = columns.get(i).type;
        }

        return new GroupByResult(resultMap, new DataFrame(colName, colType), new ArrayList<String>(Arrays.asList(colNames)));
    }

    public GroupByResult groupByMT(String[] colNames) throws InterruptedException {
        HashMap<ArrayList<Value>,DataFrame> resultMap = new HashMap<>();

        int columnsSize;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        for (int i=100000; i<=size(); i+=100000)
        {
            final int finalSize = i;
            Thread t = new Thread(() -> {
                Value[] fullRaw = new Value[columns.size()];
                ArrayList<Value> keyRaw = new ArrayList<>();
                int counter=0;
                for (int z = finalSize-100000; z < finalSize; z++) {
                    counter++;
                    keyRaw.clear();
                    for (int k = 0, j = 0; k < columns.size(); k++) {
                        fullRaw[k] = columns.get(k).records.get(z);
                        if (j < colNames.length && columns.get(k).name.equals(colNames[j])) {
                            keyRaw.add(columns.get(k).records.get(z));
                            j++;
                        }
                    }

                    synchronized (resultMap) {
                        if (!resultMap.containsKey(keyRaw)) {
                            DataFrame df = iloc(z);
                            resultMap.put(keyRaw,df);
                        }
                        else {
                            resultMap.get(keyRaw).add(fullRaw);
                        }
                    }

                }
                System.out.println(counter);
            });
            executor.execute(t);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {}

        int sum=0, grN=0;
        for(DataFrame df : resultMap.values()) {
            System.out.println(df.columns.get(0).records.get(0));
            sum+=df.size();
            grN++;
        }
        System.out.println(sum);
        System.out.println(grN);

        String[] colName = new String[columns.size()];
        String[] colType = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            colName[i] = columns.get(i).name;
            colType[i] = columns.get(i).type;
        }

        return new GroupByResult(resultMap, new DataFrame(colName, colType), new ArrayList<String>(Arrays.asList(colNames)));
    }
}