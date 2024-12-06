import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Node {
    private final String centralServerAddress;
    private final int centralServerPort;
    private final int nodePort;
    private final Map<String, File> registeredFiles = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    public Node(String centralServerAddress, int centralServerPort, int nodePort) {
        this.centralServerAddress = centralServerAddress;
        this.centralServerPort = centralServerPort;
        this.nodePort = nodePort;
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter central server address: ");
        String serverAddress = scanner.nextLine();
        System.out.print("Enter central server port: ");
        int serverPort = scanner.nextInt();
        System.out.print("Enter this node's port: ");
        int nodePort = scanner.nextInt();
        scanner.nextLine();

        Node node = new Node(serverAddress, serverPort, nodePort);
        node.start();
    }

    public void start() {
        // Start file receiver
        new Thread(this::startFileReceiver).start();

        // Start automatic file retrieval service
        new Thread(this::startAutoFileRetrieval).start();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("\n1. Register File\n2. Query File\n3. Send Registered File\n4. Request File\n5. List Registered Files\n6. Exit");
            System.out.print("Choose an option: ");
            int choice = scanner.nextInt();
            scanner.nextLine();

            switch (choice) {
                case 1 -> registerFile();
                case 2 -> queryFile();
                case 3 -> sendRegisteredFile();
                case 4 -> requestFile();
                case 5 -> listRegisteredFiles();
                case 6 -> {
                    System.out.println("Exiting...");
                    executorService.shutdown();
                    System.exit(0);
                }
                default -> System.out.println("Invalid choice. Try again.");
            }
        }
    }

    private void startAutoFileRetrieval() {
        while (true) {
            try {
                // Wait for 5 minutes before checking for new files
                Thread.sleep(300000); // 5 minutes = 300,000 milliseconds

                // Query for all registered files
                List<String> registeredFileNames = getRegisteredFileNames();
                for (String fileName : registeredFileNames) {
                    if (!registeredFiles.containsKey(fileName)) {
                        retrieveFile(fileName);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void listRegisteredFiles() {
        try (Socket listSocket = new Socket(centralServerAddress, centralServerPort);
             DataOutputStream listOut = new DataOutputStream(listSocket.getOutputStream());
             DataInputStream listIn = new DataInputStream(listSocket.getInputStream())) {
    
            // First, get the list of files
            listOut.writeUTF("LIST_FILES");
            
            int fileCount = listIn.readInt();
            
            if (fileCount == 0) {
                System.out.println("No files registered across all nodes.");
                return;
            }
    
            System.out.println("Registered Files across all nodes:");
            List<String> fileNames = new ArrayList<>();
            for (int i = 0; i < fileCount; i++) {
                fileNames.add(listIn.readUTF());
            }
            listSocket.close();
    
            // Now query each file separately
            for (String fileName : fileNames) {
                try (Socket querySocket = new Socket(centralServerAddress, centralServerPort);
                     DataOutputStream queryOut = new DataOutputStream(querySocket.getOutputStream());
                     DataInputStream queryIn = new DataInputStream(querySocket.getInputStream())) {
    
                    queryOut.writeUTF("QUERY");
                    queryOut.writeUTF(fileName);
    
                    String response = queryIn.readUTF();
                    System.out.println("File: " + fileName);
                    
                    if (response.startsWith("File found")) {
                        int nodeCount = queryIn.readInt();
                        System.out.println("  Available on nodes:");
                        for (int j = 0; j < nodeCount; j++) {
                            String address = queryIn.readUTF();
                            int port = queryIn.readInt();
                            System.out.println("    - " + address + ":" + port);
                        }
                    } else {
                        System.out.println("  No nodes currently hosting this file.");
                    }
                } catch (IOException e) {
                    System.err.println("Error querying file " + fileName + ": " + e.getMessage());
                }
            }
    
        } catch (IOException e) {
            System.err.println("Error getting registered files: " + e.getMessage());
        }
    }

    private List<String> getRegisteredFileNames() {
        try (Socket socket = new Socket(centralServerAddress, centralServerPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
    
            out.writeUTF("LIST_FILES");
            
            int fileCount = in.readInt();
            List<String> fileNames = new ArrayList<>();
            
            for (int i = 0; i < fileCount; i++) {
                fileNames.add(in.readUTF());
            }
            
            return fileNames;
        } catch (IOException e) {
            System.err.println("Error getting registered files: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    private void retrieveFile(String fileName) {
        executorService.submit(() -> {
            try (Socket socket = new Socket(centralServerAddress, centralServerPort);
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 DataInputStream in = new DataInputStream(socket.getInputStream())) {

                out.writeUTF("QUERY");
                out.writeUTF(fileName);

                String response = in.readUTF();
                if (response.startsWith("File found")) {
                    int nodeCount = in.readInt();
                    for (int i = 0; i < nodeCount; i++) {
                        String address = in.readUTF();
                        int port = in.readInt();

                        // Try to download the file from one of the nodes
                        if (downloadFile(address, port, fileName)) {
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Error retrieving file: " + e.getMessage());
            }
        });
    }

    private boolean downloadFile(String nodeAddress, int nodePort, String fileName) {
        try (Socket fileSocket = new Socket(nodeAddress, nodePort);
             DataOutputStream out = new DataOutputStream(fileSocket.getOutputStream());
             DataInputStream in = new DataInputStream(fileSocket.getInputStream())) {

            // Request the specific file
            out.writeUTF("DOWNLOAD");
            out.writeUTF(fileName);
            out.writeLong(Long.MAX_VALUE); // Indicate we want the entire file

            // Check if file exists
            String response = in.readUTF();
            if ("FILE_NOT_FOUND".equals(response)) {
                System.out.println("File not found on " + nodeAddress + ":" + nodePort);
                return false;
            }

            File file = new File("downloaded_" + fileName);
            try (FileOutputStream fileOut = new FileOutputStream(file)) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) > 0) {
                    fileOut.write(buffer, 0, bytesRead);
                }
            }

            System.out.println("File downloaded: " + file.getAbsolutePath());
            registeredFiles.put(fileName, file);
            return true;
        } catch (IOException e) {
            System.err.println("Failed to download file from " + nodeAddress + ":" + nodePort);
            return false;
        }
    }

    private void requestFile() {
        Scanner scanner = new Scanner(System.in);
        
        System.out.print("Enter the target node's address: ");
        String targetAddress = scanner.nextLine();
        
        System.out.print("Enter the target node's port: ");
        int targetPort = scanner.nextInt();
        scanner.nextLine();

        System.out.print("Enter the file name to request: ");
        String fileName = scanner.nextLine();

        try (Socket socket = new Socket(targetAddress, targetPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            // Send request for the specific file
            out.writeUTF("REQUEST_FILE");
            out.writeUTF(fileName);
            out.writeUTF("localhost"); // Sender will know the requester's address
            out.writeInt(nodePort);    // Sender will know the requester's port

            // Optional: Wait for confirmation
            String response = in.readUTF();
            System.out.println(response);

        } catch (IOException e) {
            System.err.println("Error requesting file: " + e.getMessage());
        }
    }

    private void registerFile() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter file path to register: ");
        String filePath = scanner.nextLine();
        File file = new File(filePath);

        if (!file.exists()) {
            System.out.println("File does not exist.");
            return;
        }

        try (Socket socket = new Socket(centralServerAddress, centralServerPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            out.writeUTF("REGISTER");
            out.writeUTF(file.getName());
            out.writeUTF("localhost");
            out.writeInt(nodePort);

            registeredFiles.put(file.getName(), file);
            System.out.println(in.readUTF());

        } catch (IOException e) {
            System.err.println("Error registering file: " + e.getMessage());
        }
    }

    private void queryFile() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter file name to query: ");
        String fileName = scanner.nextLine();

        try (Socket socket = new Socket(centralServerAddress, centralServerPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            out.writeUTF("QUERY");
            out.writeUTF(fileName);

            String response = in.readUTF();
            System.out.println(response);

            if (response.startsWith("File found")) {
                int nodeCount = in.readInt();
                for (int i = 0; i < nodeCount; i++) {
                    String address = in.readUTF();
                    int port = in.readInt();
                    System.out.println("Node: " + address + ":" + port);
                }
            }

        } catch (IOException e) {
            System.err.println("Error querying file: " + e.getMessage());
        }
    }

    private void sendRegisteredFile() {
        if (registeredFiles.isEmpty()) {
            System.out.println("No files registered yet.");
            return;
        }

        Scanner scanner = new Scanner(System.in);
        System.out.println("Select a registered file to send:");
        List<String> fileNames = new ArrayList<>(registeredFiles.keySet());
        for (int i = 0; i < fileNames.size(); i++) {
            System.out.println((i + 1) + ". " + fileNames.get(i));
        }

        System.out.print("Enter the number of the file to send: ");
        int fileIndex = scanner.nextInt() - 1;

        if (fileIndex < 0 || fileIndex >= fileNames.size()) {
            System.out.println("Invalid selection.");
            return;
        }

        String selectedFileName = fileNames.get(fileIndex);
        File fileToSend = registeredFiles.get(selectedFileName);

        System.out.print("Enter the target node's address: ");
        String targetAddress = scanner.next();
        System.out.print("Enter the target node's port: ");
        int targetPort = scanner.nextInt();

        sendFile(targetAddress, targetPort, fileToSend);
    }

    private void sendFile(String serverAddress, int port, File file) {
        try (Socket socket = new Socket(serverAddress, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             FileInputStream fileInputStream = new FileInputStream(file)) {

            out.writeUTF(file.getName());
            out.writeLong(file.length());

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fileInputStream.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }

            System.out.println("File sent successfully: " + file.getName());

        } catch (IOException e) {
            System.err.println("Error sending file: " + e.getMessage());
        }
    }

    private void startFileReceiver() {
        try (ServerSocket serverSocket = new ServerSocket(nodePort)) {
            System.out.println("File receiver running on port " + nodePort);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleFileTransfer(clientSocket)).start();
            }

        } catch (IOException e) {
            System.err.println("Error starting file receiver: " + e.getMessage());
        }
    }

    private void handleFileTransfer(Socket clientSocket) {
        try (DataInputStream in = new DataInputStream(clientSocket.getInputStream());
             DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {
            
            String action = in.readUTF();

            if ("REQUEST_FILE".equals(action)) {
                // Handle file request
                String fileName = in.readUTF();
                String requesterAddress = in.readUTF();
                int requesterPort = in.readInt();

                // Print message about file request
                System.out.println("A node on server : " + requesterAddress + 
                                   " on port : " + requesterPort + 
                                   " is requesting the file -> " + fileName);

                // Check if file exists in registered files
                File fileToSend = registeredFiles.get(fileName);
                if (fileToSend == null) {
                    out.writeUTF("File not found");
                    return;
                }

                // Confirm file request
                out.writeUTF("File request received. Preparing to send.");

                // Send the file
                try (FileInputStream fileInputStream = new FileInputStream(fileToSend)) {
                    out.writeUTF(fileToSend.getName());
                    out.writeLong(fileToSend.length());

                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = fileInputStream.read(buffer)) > 0) {
                        out.write(buffer, 0, bytesRead);
                    }
                }

                System.out.println("File " + fileName + " sent successfully.");
            } else if ("DOWNLOAD".equals(action)) {
                // Handle download request
                String fileName = in.readUTF();
                File fileToSend = registeredFiles.get(fileName);

                if (fileToSend == null) {
                    out.writeUTF("FILE_NOT_FOUND");
                    return;
                }

                out.writeUTF("FILE_FOUND");

                try (FileInputStream fileInputStream = new FileInputStream(fileToSend)) {
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = fileInputStream.read(buffer)) > 0) {
                        out.write(buffer, 0, bytesRead);
                    }
                }
            } else {
                // Existing file transfer logic
                String fileName = in.readUTF();
                long fileSize = in.readLong();

                File file = new File("received_" + fileName);
                try (FileOutputStream fileOut = new FileOutputStream(file)) {
                    byte[] buffer = new byte[4096];
                    long bytesRead = 0;
                    int read;

                    while ((read = in.read(buffer)) > 0) {
                        fileOut.write(buffer, 0, read);
                        bytesRead += read;

                        if (bytesRead >= fileSize) break;
                    }
                }

                registeredFiles.put(fileName, file);
                System.out.println("File received: " + file.getAbsolutePath());
            }
        } catch (IOException e) {
            System.err.println("Error handling file transfer: " + e.getMessage());
        }
    }
}