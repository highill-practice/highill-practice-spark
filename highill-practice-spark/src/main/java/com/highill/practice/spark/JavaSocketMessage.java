package com.highill.practice.spark;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

public class JavaSocketMessage {

	public static void main(String[] args) {
		try {
	        Socket clientSocket = new Socket("127.0.0.1", 9999);
	        
	        OutputStream outputStream = clientSocket.getOutputStream();
	        PrintWriter printWriter = new PrintWriter(outputStream);
	        
	        InputStream inputStream = clientSocket.getInputStream();
	        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
	        
	        String info = "这是测试 Hello world";
	        printWriter.write(info);
	        printWriter.flush();
	        clientSocket.shutdownOutput();
	        
	        String reply = null;
	        while(!((reply = bufferedReader.readLine()) == null)) {
	        	System.out.println("----Reply : " + reply);
	        }
	        
	        bufferedReader.close();
	        inputStream.close();
	        printWriter.close();
	        outputStream.close();
	        clientSocket.close();
	        
        } catch (Exception e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        } 

	}

}
