package main.client;

import main.dataFrame.DataFrame;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;

public class EchoClient {
    public static DataFrame calculate (DataFrame df, String operation) throws IOException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        Socket echoSocket = null;
        PrintWriter out = null;
        BufferedReader in = null;

        try {
            echoSocket = new Socket("localhost", 6666);
            out = new PrintWriter(echoSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(
                    echoSocket.getInputStream()));
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host: localhost.");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for "
                    + "the connection to: localhost.");
            System.exit(1);
        }

        BufferedReader stdIn = new BufferedReader(
                new InputStreamReader(System.in));
        String userInput;

/*        System.out.println("Type a message: ");
        while ((userInput = stdIn.readLine()) != null) {
            out.println(userInput);
            String result = in.readLine();
            System.out.println(result);
        }*/

        String stringToSend = df.serialDataFrame() + "#" + operation;
        String result;

        out.println(stringToSend);
        while ((result = in.readLine()) != null) {
            break;
        }

        out.close();
        in.close();
        stdIn.close();
        echoSocket.close();

        return DataFrame.deserialDataFrame(result);
    }
}

