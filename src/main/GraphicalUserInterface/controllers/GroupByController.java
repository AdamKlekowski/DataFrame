package main.GraphicalUserInterface.controllers;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ResourceBundle;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.TextField;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import main.client.EchoClient;
import main.dataFrame.Column;
import main.dataFrame.DataFrame;
import main.dataFrame.GroupByResult;

public class GroupByController implements Initializable {
    private DataFrame resultDf;
    @FXML
    ChoiceBox choiceList;
    @FXML
    ChoiceBox clusterList;
    @FXML
    TextField columnsToGroupBy;
    @FXML
    Button saveAndExit;


    void saveDataFrameToFile(File file) {
        try {
            PrintWriter writer;
            writer = new PrintWriter(file);

            StringBuilder line = new StringBuilder();
            boolean isFirstColumn = true;
            for (Column col : resultDf.columns) {
                if (! isFirstColumn) {
                    line.append(",");
                } else {
                    isFirstColumn = false;
                }
                line.append(col.name);
            }
            writer.println(line);

            for (int i=0; i < resultDf.size(); i++) {
                line = new StringBuilder();
                isFirstColumn = true;
                for (Column col : resultDf.columns) {
                    if (! isFirstColumn) {
                        line.append(",");
                    } else {
                        isFirstColumn = false;
                    }
                    line.append(col.records.get(i).toString());
                }
                writer.println(line);
            }

            writer.close();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    @FXML
    void save() throws InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        String[] splitedColsToGroupBy = columnsToGroupBy.getText().split(",");

        if (clusterList.getValue().toString().equals("send to server")) {
            switch (choiceList.getValue().toString()) {
                case "max()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).maxInCluster();
                    break;

                case "min()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).minInCluster();
                    break;

                case "sum()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).sum();
                    break;

                case "mean()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).mean();
                    break;

                case "std()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).std();
                    break;

                case "var()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).var();
                    break;
            }
        }
        else {
            switch (choiceList.getValue().toString()) {
                case "max()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).max();
                    break;

                case "min()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).min();
                    break;

                case "sum()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).sum();
                    break;

                case "mean()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).mean();
                    break;

                case "std()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).std();
                    break;

                case "var()":
                    resultDf = MainController.df.groupBy(splitedColsToGroupBy).var();
                    break;
            }
        }

        FileChooser fileChooser = new FileChooser();
        FileChooser.ExtensionFilter extFilter = new FileChooser.ExtensionFilter("CSV files (*.csv)", "*.csv");
        fileChooser.getExtensionFilters().add(extFilter);

        Stage saveFileStage = new Stage();
        File file = fileChooser.showSaveDialog(saveFileStage);

        if (file != null) {
            saveDataFrameToFile(file);
        }

        ((Stage)saveAndExit.getScene().getWindow()).close();
    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        String[] functionList = new String[] {"max()", "min()", "sum()", "mean()", "std()", "var()"};
        choiceList.getItems().addAll(functionList);

        functionList = new String[] {"on this machine", "send to server"};
        clusterList.getItems().addAll(functionList);
    }
}
