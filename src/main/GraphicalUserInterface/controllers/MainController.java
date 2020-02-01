package main.GraphicalUserInterface.controllers;

import java.io.*;
import java.net.URL;
import java.sql.SQLException;
import java.util.ResourceBundle;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.chart.*;
import javafx.scene.control.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import main.dataFrame.Column;
import main.dataFrame.DataFrame;
import main.dataFrame.DataFrameDB;

public class MainController implements Initializable {
    public static DataFrame df;
    @FXML TableView table;

    private void fullfilTable() {
        table.getColumns().clear();
        for(Column col : df.columns) {
            table.getColumns().add(new TableColumn<String, String>(col.name));
        }
    }

    @FXML
    void loadFile() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, InterruptedException, SQLException {
        FileChooser fileChooser = new FileChooser();
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("CSV Files", "*.csv"));
        File file = fileChooser.showOpenDialog(null);

        if (file == null) return;
        df = new DataFrame(file.getAbsolutePath(), null,null);
        fullfilTable();
    }

    @FXML
    void loadDb() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        DataFrameDB.connect(new String[]{"id", "date", "total", "value"}, new String[]{"StringValue", "StringValue", "DoubleValue", "DoubleValue"});

        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("DataFrame loaded");
        alert.setHeaderText(null);
        alert.setContentText("Date was successfully loaded from database");

        alert.show();
    }

    private boolean isFileLoaded(){
        if (df == null) {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("DataFrame not found");
            alert.setHeaderText(null);
            alert.setContentText("You have to load file!");

            alert.show();
            return false;
        }
        else {
            return true;
        }
    }

    @FXML
    void groupBy(ActionEvent event) throws InterruptedException, IllegalAccessException, InstantiationException, ClassNotFoundException, IOException {
        if(! isFileLoaded()) return;

        FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("groupby.fxml"));
        Parent root = fxmlLoader.load();
        Stage stage = new Stage();
        stage.setTitle("Group By Settings");
        stage.setScene(new Scene(root));
        stage.show();
    }


    @FXML
    void test(ActionEvent event) throws InterruptedException, IllegalAccessException, InstantiationException, ClassNotFoundException, SQLException {
        if(! isFileLoaded()) return;

        Stage plotStage = new Stage();
        long start, stop;

        plotStage.setTitle("Performance Test Plot");
        final CategoryAxis xAxis = new CategoryAxis();
        final NumberAxis yAxis = new NumberAxis();
        final BarChart<String,Number> bc =
                new BarChart<String,Number>(xAxis,yAxis);
        bc.setTitle("Performance Test");
        xAxis.setLabel("Operations");
        yAxis.setLabel("Time (milliseconds)");

        XYChart.Series series1 = new XYChart.Series();
        XYChart.Series series2 = new XYChart.Series();
        series1.setName("single thread");
        series2.setName("multi threads");

        int amount_of_tests = 3;
        long test1=0, test2=0, test3=0, test4=0, test5=0, test6=0, test7=0, test8=0;
        for(int i=0; i < amount_of_tests; i++) {
            //--------------------------------------------------------------
            start = System.currentTimeMillis();
            df.groupBy(new String[]{"id"}).maxWithoutMultiThreading();
            stop = System.currentTimeMillis();
            test1 += stop - start;
            //--------------------------------------------------------------
            start = System.currentTimeMillis();
            df.groupBy(new String[]{"id"}).max();
            stop = System.currentTimeMillis();
            test2 += stop - start;
            //--------------------------------------------------------------
            start = System.currentTimeMillis();
            df.groupBy(new String[]{"id"}).minWithoutMultiThreading();
            stop = System.currentTimeMillis();
            test3 += stop - start;
            //--------------------------------------------------------------
            start = System.currentTimeMillis();
            df.groupBy(new String[]{"id"}).min();
            stop = System.currentTimeMillis();
            test4 += stop - start;
            //--------------------------------------------------------------
            start = System.currentTimeMillis();
            df.groupBy(new String[]{"id"}).meanWithoutMultiThreading();
            stop = System.currentTimeMillis();
            test5 += stop - start;
            //--------------------------------------------------------------
            start = System.currentTimeMillis();
            df.groupBy(new String[]{"id"}).mean();
            stop = System.currentTimeMillis();
            test6 += stop - start;
            //--------------------------------------------------------------
            start = System.currentTimeMillis();
            df.groupBy(new String[]{"id"}).sumWithoutMultiThreading();
            stop = System.currentTimeMillis();
            test7 += stop - start;
            //--------------------------------------------------------------
            start = System.currentTimeMillis();
            df.groupBy(new String[]{"id"}).sum();
            stop = System.currentTimeMillis();
            test8 += stop - start;
        }
        series1.getData().add(new XYChart.Data("max", test1/amount_of_tests));
        series2.getData().add(new XYChart.Data("max", test2/amount_of_tests));
        series1.getData().add(new XYChart.Data("min", test3/amount_of_tests));
        series2.getData().add(new XYChart.Data("min", test4/amount_of_tests));
        series1.getData().add(new XYChart.Data("mean", test5/amount_of_tests));
        series2.getData().add(new XYChart.Data("mean", test6/amount_of_tests));
        series1.getData().add(new XYChart.Data("sum", test7/amount_of_tests));
        series2.getData().add(new XYChart.Data("sum", test8/amount_of_tests));

        Scene scene  = new Scene(bc,800,700);
        bc.getData().addAll(series1, series2);
        plotStage.setScene(scene);
        plotStage.show();
    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
    }
}
