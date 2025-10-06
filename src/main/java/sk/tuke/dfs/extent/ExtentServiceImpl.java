package sk.tuke.dfs.extent;

import com.google.protobuf.ByteString;
import com.google.protobuf.Field;
import dfs.extent.ExtentServiceGrpc;
import dfs.extent.ExtentServiceOuterClass;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class ExtentServiceImpl extends ExtentServiceGrpc.ExtentServiceImplBase {

    private String extentRoot;
    Logger logger = Logger.getLogger(ExtentServiceImpl.class.getName());

    public ExtentServiceImpl(String extentRoot) throws IOException {
        super();
        this.extentRoot = extentRoot;
        Files.createDirectories(Path.of(extentRoot));
    }

    @Override
    public void stop(ExtentServiceOuterClass.StopRequest request, StreamObserver<ExtentServiceOuterClass.StopResponse> responseObserver) {
        System.exit(0);
    }

    @Override
    public void get(ExtentServiceOuterClass.GetRequest request, StreamObserver<ExtentServiceOuterClass.GetResponse> responseObserver) {
        String fileName = request.getFileName();
        logger.log(Level.INFO, "Getting file/directory " + fileName);

        if (fileName.endsWith("/") || fileName.isEmpty()) {
            // Case for directory
            if (fileName.startsWith("/") && fileName.length() > 1)
                fileName = fileName.substring(1);
            Path path = Paths.get(extentRoot);
            Path real;
            if (fileName.length() == 1)
                real = path;
            else
                real = path.resolve(fileName);

            logger.log(Level.INFO, "Directory path " + real);
            try {
                if (Files.isDirectory(real)) {
                    Stream<Path> s = Files.list(real);
                    List<String> names = s.map(child -> {
                        String n = child.getFileName().toString();
                        return Files.isDirectory(child) ? n + "/" : n;
                    }).sorted().toList();

                    String joined = String.join(System.lineSeparator(), names);

                    ExtentServiceOuterClass.GetResponse response = ExtentServiceOuterClass.GetResponse
                            .newBuilder()
                            .setFileData(ByteString.copyFrom(joined.getBytes()))
                            .build();
                    logger.log(Level.FINE, "Get response" + response);
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            } catch (IOException e) {
                System.out.println("dsadsadas");
                responseObserver.onCompleted();
            }
        } else {
            // Case for file
            if(fileName.startsWith("/"))
                fileName = fileName.substring(1);
            Path path = Paths.get(extentRoot);
            Path real = path.resolve(fileName).normalize();

            logger.log(Level.INFO, "Real path for file is " + real);
            if (!Files.isRegularFile(real)) {
                responseObserver.onNext(ExtentServiceOuterClass.GetResponse.getDefaultInstance());
                responseObserver.onCompleted();
                return;
            }
            try {
                byte[] bytes = Files.readAllBytes(real);
                ExtentServiceOuterClass.GetResponse response = ExtentServiceOuterClass.GetResponse
                        .newBuilder()
                        .setFileData(ByteString.copyFrom(bytes))
                        .build();

                logger.log(Level.INFO, "Get response " + response.getFileData().toStringUtf8());

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (IOException e) {
                e.printStackTrace();
                responseObserver.onNext(ExtentServiceOuterClass.GetResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void put(ExtentServiceOuterClass.PutRequest request, StreamObserver<ExtentServiceOuterClass.PutResponse> responseObserver) {
        String fileName = request.getFileName();
        logger.log(Level.INFO, "file/directory name is " + fileName);
        if (fileName.endsWith("/") || fileName.isEmpty()) {
            // Case directory
            try {
                if (fileName.startsWith("/"))
                    fileName = fileName.substring(1);
                Path path = Paths.get(extentRoot);
                Path real;
                // with and without name
                if (fileName.length() == 1)
                    real = path;
                else
                    real = path.resolve(fileName).normalize();

                if (!request.hasFileData()) {
                    if (!Files.exists(real)) {
                        responseObserver.onNext(ExtentServiceOuterClass.PutResponse.newBuilder().setSuccess(false).build());
                        responseObserver.onCompleted();
                        return;
                    }
                    try (Stream<Path> s = Files.list(real)) {
                        boolean empty = s.findAny().isEmpty();
                        if (empty) {
                            Files.delete(real);
                            responseObserver.onNext(ExtentServiceOuterClass.PutResponse.newBuilder().setSuccess(true).build());
                            responseObserver.onCompleted();
                        } else {
                            logger.log(Level.WARNING, "Directory " + real + " is not empty");
                            responseObserver.onNext(ExtentServiceOuterClass.PutResponse.newBuilder().setSuccess(false).build());
                            responseObserver.onCompleted();
                        }
                    }
                } else {
                    logger.log(Level.INFO, "creating a directory " + real);
                    if (Files.exists(real)) {
                        logger.log(Level.WARNING, "Directory " + real + " is already exist");
                        responseObserver.onNext(ExtentServiceOuterClass.PutResponse.newBuilder().setSuccess(false).build());
                        responseObserver.onCompleted();
                        return;
                    }
                    logger.log(Level.INFO, "Directory doesn't exist");
                    Files.createDirectories(real);
                    if (Files.exists(real))
                        logger.log(Level.INFO, "Directory was created " + real);
                    responseObserver.onNext(ExtentServiceOuterClass.PutResponse.newBuilder().setSuccess(true).build());
                    responseObserver.onCompleted();
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, "Put error " + e);
                responseObserver.onNext(ExtentServiceOuterClass.PutResponse.newBuilder().setSuccess(false).build());
                responseObserver.onCompleted();
            }
        } else {
            // Case file
            try {
                if (fileName.startsWith("/"))
                    fileName = fileName.substring(1);

                Path root = Paths.get(extentRoot);
                Path real = root.resolve(fileName).normalize();
                byte[] fileData = request.getFileData().toByteArray();

                logger.log(Level.INFO, "Real path for put is " + real);

                if (!request.hasFileData() && Files.exists(real)) {
                    Files.delete(real);
                    responseObserver.onNext(ExtentServiceOuterClass.PutResponse.newBuilder().setSuccess(true).build());
                    responseObserver.onCompleted();
                    return;
                } else {
                    if (Files.exists(real)) {
                        responseObserver.onNext(ExtentServiceOuterClass.PutResponse.newBuilder().setSuccess(false).build());
                        responseObserver.onCompleted();
                        return;
                    }
                    Files.write(real, fileData, StandardOpenOption.CREATE);
                    responseObserver.onNext(ExtentServiceOuterClass.PutResponse.newBuilder().setSuccess(true).build());
                    responseObserver.onCompleted();
                }


            } catch (IOException e) {
                logger.log(Level.WARNING, "Put error");
                responseObserver.onNext(ExtentServiceOuterClass.PutResponse.newBuilder().setSuccess(false).build());
                responseObserver.onCompleted();
            }
        }
    }
}
