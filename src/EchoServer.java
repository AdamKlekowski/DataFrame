import java.io.*;
import java.net.*;

public class EchoServer {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(6666);
        } catch (IOException e) {
            System.out.println("Could not listen on port: 6666");
            System.exit(-1);
        }

        Socket mainSocket = null;
        Socket clientToCalculate = null;
        try {
            clientToCalculate = serverSocket.accept();
        } catch (IOException e) {
            System.out.println("Accept failed: 6666");
            System.exit(-1);
        }
        System.out.println("cluster works");


        while(true) {
            try {
                mainSocket = serverSocket.accept();
            } catch (IOException e) {
                System.out.println("Accept failed: 6666");
                System.exit(-1);
            }
            System.out.println("main client works");

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            mainSocket.getInputStream()));

            PrintWriter out = new PrintWriter(clientToCalculate.getOutputStream(), true);

            PrintWriter out_main = new PrintWriter(mainSocket.getOutputStream(), true);
            String inputLine;

            BufferedReader in_calculate = new BufferedReader(
                    new InputStreamReader(
                            clientToCalculate.getInputStream()));

            while ((inputLine = in.readLine()) != null) { //przyjmuje od maina
                out.println(inputLine); //wysyla do claculate ...
                String line = in_calculate.readLine(); //przyjmuje od calculate...
                out_main.println(line);
            }

            //out.close();
            in.close();
            out_main.close();
            mainSocket.close();
            //serverSocket.close();
        }


    }
}

