package main;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Stage;

/**
 * @author Adam Klekowski
 * AGH University of Science and Technology
 * Cracow, Poland
 */

public class MainProgram extends Application {

    @Override
    public void start(Stage stage) throws Exception {
        Parent root = FXMLLoader.load(getClass().getResource("GraphicalUserInterface/views/view.fxml"));

        stage.setTitle("Data Frame");
        stage.setScene(new Scene(root));
        stage.getIcons().add(new Image("file:icon.png"));
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
