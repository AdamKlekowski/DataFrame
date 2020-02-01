package main.dataFrame;

import main.value.IntegerValue;
import main.value.Value;

import java.util.ArrayList;

/**
 *
 * size - number of raws
 */
public class SparseDataFrame extends DataFrame {
    private String hide;
    private int length;

    public SparseDataFrame(DataFrame dfToParse, String hide) {
        this.hide=hide;
        length =dfToParse.size();

        ArrayList<C00Value> tmp = new ArrayList<>();

        columns = new ArrayList<>();
        for (int i=0; i<dfToParse.columns.size(); i++) {
            columns.add(new Column(dfToParse.columns.get(i).name, dfToParse.columns.get(i).type));

            for (int j=0; j<dfToParse.size(); j++) {
                if (! dfToParse.columns.get(i).records.get(j).equals(hide)) {
                    tmp.add(new C00Value(j, dfToParse.columns.get(i).records.get(j)));
                }
            }

            for (C00Value element : tmp) {
                columns.get(i).records.add(element);
            }
            tmp.clear();
        }
    }

    public int size() {
        return length;
    }

    private int getC00Index(int indexCheckingColumn, int checkingPlace) {
        ArrayList<Value> checkingColumn = columns.get(indexCheckingColumn).records;
        for (int k=0; k<checkingColumn.size(); k++) {
            C00Value tmp = (C00Value) checkingColumn.get(k);
            if (tmp.getPlace() == checkingPlace) {
                return k;
            }
        }
        return -1;
    }

    /**
     * Printing data frame names and columns.
     */
    public void print(int limit) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        this.toDense().print(limit);
    }

    public DataFrame toDense() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        DataFrame newDf = new DataFrame();
        newDf.columns = new ArrayList<>();

        for (int i=0; i<columns.size(); i++) {
            newDf.columns.add(new Column(columns.get(i).name, columns.get(i).type));
        }

        Value newDFLine[] = new Value[columns.size()]; //TODO
        for(int i = 0; i< length; i++) {
            for (int j = 0; j < columns.size(); j++) {
                if (getC00Index(j, i) != -1) {
                    Class c=Class.forName(columns.get(j).type);
                    IntegerValue s=(IntegerValue) c.newInstance();

                    C00Value foundC00Value = (C00Value)columns.get(j).records.get(getC00Index(j, i));
                    newDFLine[j] = s.create((String) foundC00Value.getHiddenValue());
                }
                else {
                    Class c=Class.forName(columns.get(j).type);
                    IntegerValue s=(IntegerValue) c.newInstance();

                    newDFLine[j] = s.create(hide);
                }
            }
            newDf.add(newDFLine);
        }
        return newDf;
    }
}
