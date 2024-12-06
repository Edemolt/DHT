// import java.io.*;
// import java.net.*;
// import java.util.*;

// public class CentralServer {
//     private final Map<String, List<NodeInfo>> fileRegistry = new HashMap<>();

//     public static void main(String[] args) {
//         new CentralServer().startServer(8000);
//     }

//     void startServer(int port) {
//         try (ServerSocket serverSocket = new ServerSocket(port)) {
//             System.out.println("Central Server running on port " + port);
//             while (true) {
//                 Socket socket = serverSocket.accept();
//                 new Thread(() -> handleClient(socket)).start();
//             }
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//     }

//     private synchronized void registerFile(String fileName, NodeInfo node) {
//         fileRegistry.computeIfAbsent(fileName, k -> new ArrayList<>()).add(node);
//         System.out.println("Registered: " + fileName + " -> " + node.address + ":" + node.port);
//     }

//     private synchronized List<NodeInfo> queryFile(String fileName) {
//         return fileRegistry.get(fileName);
//     }

//     void handleClient(Socket clientSocket) {
//         try (DataInputStream in = new DataInputStream(clientSocket.getInputStream());
//              DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {
            
//             String action = in.readUTF();

//             if ("REGISTER".equalsIgnoreCase(action)) {
//                 String fileName = in.readUTF();
//                 String nodeAddress = in.readUTF();
//                 int nodePort = in.readInt();

//                 registerFile(fileName, new NodeInfo(nodeAddress, nodePort));
//                 out.writeUTF("File Registered Successfully");
//             } else if ("QUERY".equalsIgnoreCase(action)) {
//                 String fileName = in.readUTF();
//                 List<NodeInfo> nodes = queryFile(fileName);
                
//                 if (nodes == null) {
//                     out.writeUTF("File not found");
//                 } else {
//                     out.writeUTF("File found on below nodes");
//                     out.writeInt(nodes.size());
//                     for (NodeInfo node : nodes) {
//                         out.writeUTF(node.address);
//                         out.writeInt(node.port);
//                     }
//                 }
//             }
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//     }
// }


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class CentralServer {
    private final Map<String, List<NodeInfo>> fileRegistry = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        int port = 8000;
        System.out.println("Starting Central Server on port " + port);
        new CentralServer().startServer(port);
    }

    void startServer(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Central Server running on port " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(() -> handleClient(socket)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized void registerFile(String fileName, NodeInfo node) {
        fileRegistry.computeIfAbsent(fileName, k -> new ArrayList<>()).add(node);
        System.out.println("Registered: " + fileName + " -> " + node.address + ":" + node.port);
    }

    private synchronized List<NodeInfo> queryFile(String fileName) {
        return fileRegistry.get(fileName);
    }

    void handleClient(Socket clientSocket) {
        try (DataInputStream in = new DataInputStream(clientSocket.getInputStream());
             DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {
            
            String action = in.readUTF();

            if ("REGISTER".equalsIgnoreCase(action)) {
                String fileName = in.readUTF();
                String nodeAddress = in.readUTF();
                int nodePort = in.readInt();

                registerFile(fileName, new NodeInfo(nodeAddress, nodePort));
                out.writeUTF("File Registered Successfully");
            } else if ("QUERY".equalsIgnoreCase(action)) {
                String fileName = in.readUTF();
                List<NodeInfo> nodes = queryFile(fileName);
                
                if (nodes == null || nodes.isEmpty()) {
                    out.writeUTF("File not found");
                } else {
                    out.writeUTF("File found on below nodes");
                    out.writeInt(nodes.size());
                    for (NodeInfo node : nodes) {
                        out.writeUTF(node.address);
                        out.writeInt(node.port);
                    }
                }
            } else if ("LIST_FILES".equalsIgnoreCase(action)) {
                // Return list of all registered files
                List<String> fileNames = new ArrayList<>(fileRegistry.keySet());
                out.writeInt(fileNames.size());
                for (String fileName : fileNames) {
                    out.writeUTF(fileName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}