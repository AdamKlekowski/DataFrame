import main.dataFrame.Column;
import main.dataFrame.DataFrame;
import main.value.Value;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

public class ClientToCalculate {
    public static void main(String[] args) throws IOException, InterruptedException, IllegalAccessException, InstantiationException, ClassNotFoundException {

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

        System.out.println("Waiting");
/*        while ((userInput = stdIn.readLine()) != null) {
            out.println(userInput);
            System.out.println("echo: " + in.readLine());
        }*/

        String line;
        while ((line = in.readLine()) != null) {
            System.out.print("working ");
            String[] splitedLine = line.split("#");

            DataFrame df = DataFrame.deserialDataFrame(splitedLine[0]);
            switch (splitedLine[1]) {
                case "max":
                    df = maxWithoutMultiThreading(df);
                    break;
            }
            out.println(df.serialDataFrame());
            System.out.println("- result sent");
        }
        System.out.println("End");

/*        out.close();
        in.close();
        stdIn.close();
        echoSocket.close();*/
    }

    private static DataFrame maxWithoutMultiThreading(DataFrame df) {
        String[] names = new String[df.columns.size()];
        for (int i = 0; i< df.columns.size(); i++) {
            names[i] = df.columns.get(i).name;
        }

        String[] types = new String[df.columns.size()];
        for (int i = 0; i< df.columns.size(); i++) {
            types[i] = df.columns.get(i).type;
        }
        DataFrame resultDataFrame = new DataFrame(names, types);

        Value[] rawValue = new Value[df.columns.size()];

            for (int i = 0; i < df.columns.size(); i++) {
                rawValue[i] = df.columns.get(i).records.get(0);
                for (Value rawElement : df.columns.get(i).records) {
                    if (rawElement.gte(rawValue[i])) rawValue[i] = rawElement;
                }
            }
            resultDataFrame.add(rawValue);
        return resultDataFrame;
    }
}
