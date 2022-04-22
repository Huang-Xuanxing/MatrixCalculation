package com.example.grpc.client.grpcclient;

import com.example.grpc.server.grpcserver.MatrixRequest;
import com.example.grpc.server.grpcserver.MatrixReply;
import com.example.grpc.server.grpcserver.MatrixServiceGrpc;

import com.example.grpc.server.grpcserver.PingRequest;
import com.example.grpc.server.grpcserver.PongResponse;
import com.example.grpc.server.grpcserver.PingPongServiceGrpc;

import com.example.grpc.client.grpcclient.exception.InputDataNotEnoughException;
import com.example.grpc.client.grpcclient.exception.InputFormatNotFitException;
import com.example.grpc.client.grpcclient.exception.DimensionNotFitException;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import net.devh.boot.grpc.client.inject.GrpcClient;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File; 
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
import java.util.ArrayList;
import java.text.DecimalFormat;
import java.util.Random;

import com.example.grpc.client.model.FileUploadResponse;
import org.springframework.web.multipart.MultipartFile;

@Service
public class GRPCClientService {

        private String fileName;
        private String uploadFilePath;
        private String contentType;
        private File dir;

        public String ping() {
        	ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9090)
                .usePlaintext()
                .build();        
		PingPongServiceGrpc.PingPongServiceBlockingStub stub
                = PingPongServiceGrpc.newBlockingStub(channel);        
		PongResponse helloResponse = stub.ping(PingRequest.newBuilder()
                .setPing("")
                .build());        
		channel.shutdown();        
		return helloResponse.getPong();
        }
        //To judge whether the dimension is power of 2 or not.
        private static boolean isPow(int x) {
            for (int i = 1; i < 11; i++)// detect the dimension of matrix.
                if ((1 << i) == x) return true;
            return false;
        }
        
        // File upload endpoint 
        public static List<int[][]> parseFromFile(MultipartFile file) throws DimensionNotFitException, InputFormatNotFitException,InputDataNotEnoughException
        {
            List<int[][]> ret = new ArrayList<>();
            try {
                InputStreamReader inputStrReader = new InputStreamReader(file.getInputStream());
                
                BufferedReader buffReader = new BufferedReader(inputStrReader);
                String line = "";
                int[][] a=null; 
                int[][] b=null;
                int[][] target=null;
                int row = 0;
                boolean finish = false;
                while ((line = buffReader.readLine()) != null) {
                    if (line.isEmpty()) {
                        if (a != null) {
                            target = b;
                            row = 0;
                        }
                        continue;
                    }
                    String[] matrix = line.split(" ");
                    if (a == null) {
                        if (isPow(matrix.length)) {
                            a = new int[matrix.length][matrix.length];
                            b = new int[matrix.length][matrix.length];
                            target = a;
                        } else {
                            throw new DimensionNotFitException();//If dimension not fit, throw the exception
                        }
                    }
                    for (int i = 0; i < matrix.length; i++)
                        target[row][i] = Integer.parseInt(matrix[i]);
                    ++row;
                    if (row == target.length) {
                        if (target == b) {
                            finish = true;
                            break;
                        }
                        else {
                            row = 0;
                            target = b;
                        }
                    }
                }
                if (!finish)
                    throw new InputDataNotEnoughException();//If just input one of the matrix, throw this exception
                ret.add(a);
                ret.add(b);
            } catch (IOException e) {
                e.printStackTrace();
                throw new InputFormatNotFitException();
            }
            return ret;
        }
        
        // calculate footprint of processing a block
        private static double footPrint(MatrixServiceGrpc.MatrixServiceBlockingStub stub, int a, int b){

                double startTime = System.nanoTime();
                MatrixReply temp=stub.multiplyBlock(MatrixRequest.newBuilder().setA(a).setB(b).build());
                double endTime = System.nanoTime();
                double footprint= endTime-startTime;
                return (footprint/1000000000);// change nano seconds into seconds
        }
        
        //This method processes the input matrix
        public FileUploadResponse fileUpload(@RequestParam("file") MultipartFile file,@RequestParam("deadline") double deadline) 
        {
                
        	  List<int[][]> arrs = null;
        	  int[][] matrixA;
        	  int[][] matrixB;
              try {
                  arrs = parseFromFile(file);//List of matrix that recieved
                  matrixA=arrs.get(0);
                  matrixB=arrs.get(1);
                  
                  //save file into the server
                  fileName = file.getOriginalFilename(); // get file name 
                  String filePathServer = "/home/user/DistributedMatrixMultiplication/grpc-clientuploadedFile"; // use to save file for server development 
                  uploadFilePath = filePathServer;
                  contentType = file.getContentType();
                  dir = new File(uploadFilePath + '/' + fileName);
                  if (!dir.getParentFile().exists())  dir.getParentFile().mkdirs(); // make directory if doesn't exist
                  try { file.transferTo(dir); }
                  catch (Exception e) { return new FileUploadResponse(fileName, contentType, "Please add a file! " + e.getMessage()); }
              } catch (DimensionNotFitException e) {
                  return new FileUploadResponse(fileName,contentType, "dimension not fit.");
              } catch (InputFormatNotFitException e) {
                  return new FileUploadResponse(fileName,contentType, "input file format not fit.");
              }catch (InputDataNotEnoughException e){
                  return new FileUploadResponse(fileName,contentType, "input data is not enough");
              }
              grpcClient(matrixA, matrixB, deadline);
              return new FileUploadResponse(fileName, contentType, "File is uploaded, the calculation start.");
               
        }

        //Method of matrix calculation
        public void matrixCalculation(ArrayList<MatrixServiceGrpc.MatrixServiceBlockingStub> stubArray, int server_needed, int stubs_index, int i, int j, int k, int[][] a, int[][] b, int[][] c)
        {
        	MatrixReply temp=stubArray.get(stubs_index).multiplyBlock(MatrixRequest.newBuilder().setA(a[i][k]).setB(b[k][j]).build());
            if(stubs_index == server_needed-1) stubs_index = 0;
            else stubs_index++;
            MatrixReply temp2=stubArray.get(stubs_index).addBlock(MatrixRequest.newBuilder().setA(c[i][j]).setB(temp.getC()).build());
            c[i][j] = temp2.getC();
            if(stubs_index == server_needed-1) stubs_index = 0;
            else stubs_index++;
        }
        
        //This method implements the mean function of REST services
        public void grpcClient(int[][]a, int[][]b, double deadline){
                //Setting different IP
                String IP1 = "localhost"; 
                /*String IP2 = ""; 
                String IP3 = ""; 
                String IP4 = ""; 
                String IP5 = ""; 
                String IP6 = ""; 
                String IP7 = ""; 
                String IP8 = "";*/
                //create channel
                ManagedChannel channel1 = ManagedChannelBuilder.forAddress(IP1, 12000).usePlaintext().build();  
                ManagedChannel channel2 = ManagedChannelBuilder.forAddress(IP1, 12000).usePlaintext().build();  
                ManagedChannel channel3 = ManagedChannelBuilder.forAddress(IP1, 12000).usePlaintext().build();  
                ManagedChannel channel4 = ManagedChannelBuilder.forAddress(IP1, 12000).usePlaintext().build();  
                ManagedChannel channel5 = ManagedChannelBuilder.forAddress(IP1, 12000).usePlaintext().build();  
                ManagedChannel channel6 = ManagedChannelBuilder.forAddress(IP1, 12000).usePlaintext().build();  
                ManagedChannel channel7 = ManagedChannelBuilder.forAddress(IP1, 12000).usePlaintext().build();  
                ManagedChannel channel8 = ManagedChannelBuilder.forAddress(IP1, 12000).usePlaintext().build();  
                //Scaling
                MatrixServiceGrpc.MatrixServiceBlockingStub stub1 = MatrixServiceGrpc.newBlockingStub(channel1);
                MatrixServiceGrpc.MatrixServiceBlockingStub stub2 = MatrixServiceGrpc.newBlockingStub(channel2);
                MatrixServiceGrpc.MatrixServiceBlockingStub stub3 = MatrixServiceGrpc.newBlockingStub(channel3);
                MatrixServiceGrpc.MatrixServiceBlockingStub stub4 = MatrixServiceGrpc.newBlockingStub(channel4);
                MatrixServiceGrpc.MatrixServiceBlockingStub stub5 = MatrixServiceGrpc.newBlockingStub(channel5);
                MatrixServiceGrpc.MatrixServiceBlockingStub stub6 = MatrixServiceGrpc.newBlockingStub(channel6);
                MatrixServiceGrpc.MatrixServiceBlockingStub stub7 = MatrixServiceGrpc.newBlockingStub(channel7);
                MatrixServiceGrpc.MatrixServiceBlockingStub stub8 = MatrixServiceGrpc.newBlockingStub(channel8);

                // Use java list to clollect all the stubs. 
                ArrayList<MatrixServiceGrpc.MatrixServiceBlockingStub> stubArray = new ArrayList<MatrixServiceGrpc.MatrixServiceBlockingStub>();
                stubArray.add(stub1); stubArray.add(stub2); stubArray.add(stub3); stubArray.add(stub4);
                stubArray.add(stub5); stubArray.add(stub6); stubArray.add(stub7); stubArray.add(stub8);

                
                int N = a.length;//recording the dimension

                // use a random stub from the stub array to calculate footprint 
                Random ran = new Random();
                int low_server= 0;
                int high_server= 8;
                DecimalFormat df = new DecimalFormat("#.##"); 
                int random = ran.nextInt(high_server-low_server) + low_server;
                double footprint = Double.valueOf(df.format(footPrint(stubArray.get(random), a[0][0], a[N-1][N-1])));
                
                //Calculate the server that needed in matrix calculation
                int number_of_calls = (int) Math.pow(N, 2);
                int server_needed = (int) Math.round((number_of_calls*footprint)/deadline);

                System.out.println("Deadline: " + deadline + " s"); 
                System.out.println("Server needed: " + server_needed);
                System.out.println("Footprint: " + footprint + " s");
                
                //iF the server needed out of 8, stop processing
                if((server_needed > 8) ){
                      System.out.println("The server that needed is more than the server that is running, calculation cannot be proceeded!");
                        return;
                }

                int stubs_index = 0;
                // Start calculating the matrix
                int c[][] = new int[N][N];
                for (int i = 0; i < N; i++) {
                        for (int j = 0; j < N; j++) {
                            for (int k = 0; k < N; k++) {
                                matrixCalculation(stubArray, server_needed, stubs_index,i,j,k,a,b,c);
                            }
                        }
                    }

                    // Print calculation result
                    for (int i = 0; i < a.length; i++) {
                        for (int j = 0; j < a[0].length; j++) {
                            System.out.print(c[i][j] + " ");
                        }
                        System.out.println("");
                    }
                System.out.println("----------------------------\n");
                // Close channels
                channel1.shutdown();
                channel2.shutdown();
                channel3.shutdown();
                channel4.shutdown();
                channel5.shutdown();
                channel6.shutdown();
                channel7.shutdown();
                channel8.shutdown();
                
        }
        
}
