package com.enclave.jain.udbhav;
import org.json.JSONException;
import org.json.JSONObject;

import javax.net.ssl.SSLServerSocketFactory;
import java.io.Console;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by udbhav on 17/6/17.
 */
public class EnclaveServer {


    private Connection dbConn;
    private Statement dbStatement;
    private ServerSocket serverSocket;
    private Timer CleanupTask;
    private ConcurrentHashMap<String,User> UserList;
    private ConcurrentHashMap<String,ObjectOutputStream> OutMap;
    private ConcurrentHashMap<UUID,ClientThread> ReadMap;
    private static final String logout_msg = "16c1a9c2-a9d2-4ac3-a7b2-7065ffc5a57c";
    private static final String MessageID = "MID";
    private static final String MessageTime ="TIME";
    private static final String MessageDate ="DATE";
    private static final String MessageSender ="FROM";
    private static final String SenderHash = "HASH";
    private static final String MessageContent = "MSG";
    private static final String LoginName = "NAME";
    private static final String LoginUUID = "UUID";
    private static final String LoginHash = "ID";
    private static final String HashAlgo = "SHA-256";
    private static final Long CleanupTime = Long.decode("900000");
    private static final Long LogoutTime = Long.decode("259200000");
    private static DateFormat TimeFormatter;
    private static DateFormat DateFormatter;
    private static PreparedStatement InsertMessagesStatement;
    private static PreparedStatement GetMessageTime;
    private static final String ServerName = "SERVER";
    private static final String ServerHash = "SERVER";
    private static final String UserLoginMsg =" has joined the server.";
    private static final String UserLogoutMsg = " has left the server.";
    private static PreparedStatement RemoveUser;
    private static PreparedStatement CreateUserEntry;


    private class User
    {
        String name;
        UUID userID;
        ObjectOutputStream writer;
        ObjectInputStream reader;
        String userHash;
        String loginHash;
        UUID sessionID;

    }



    private EnclaveServer(String dbAddr, String dbUname, String dbPass, String KeyStore, String keyStorePass, String portNo)
    {

        System.setProperty("javax.net.ssl.keyStore", KeyStore);
        System.setProperty("javax.net.ssl.keyStorePassword", keyStorePass);
        UserList = new ConcurrentHashMap<>();
        OutMap = new ConcurrentHashMap<>();
        ReadMap = new ConcurrentHashMap<>();
        CleanupTask = new Timer();
        CleanupTask.schedule(new TimerTask() {
            @Override
            public void run()
            {

                cleanMessageTable();

            }
        },CleanupTime,CleanupTime);


        try {
            dbConn = DriverManager.getConnection("jdbc:mysql://"+dbAddr+"?useSSL=false", dbUname, dbPass);

            dbStatement = dbConn.createStatement();

            InsertMessagesStatement = dbConn.prepareStatement("insert into messages values(?,?,?,?,?)");
            //InsertUserLog = dbConn.prepareStatement("insert into ? values(?)");
            //ClearUserLog = dbConn.prepareStatement("delete from ? where msgID like ?");
            // DropUserTable = dbConn.prepareStatement("drop table ?");
            RemoveUser = dbConn.prepareStatement("delete from users where name like ?");
            CreateUserEntry = dbConn.prepareStatement("insert into users values(?,?,?)");
            GetMessageTime = dbConn.prepareStatement("select time from messages where msgID like ?");
            // CreateUserTable = dbConn.prepareStatement("create table ?(msgID varchar(36))");
            // RetrieveBackMessages = dbConn.prepareStatement("select messages.msgID,messages.sender," +
            //         "messages.hash,messages.msg,messages.time from messages,?"+
            //          "where messages.msgID=?.msgID;");

        }catch(SQLException sqx)
        {
            //sqx.printStackTrace();
            System.out.println("[" + new Date() + "] Error connecting to database!");
            System.exit(101);
        }

        DateFormatter = new SimpleDateFormat("dd-MM-yyyy");
        TimeFormatter = new SimpleDateFormat("HH:mm:ss");


        tablesInit();




        try{
            int port = Integer.parseInt(portNo);
            serverSocket = SSLServerSocketFactory.getDefault().createServerSocket(port);

        }catch(IOException|NumberFormatException iox)
        {
            //iox.printStackTrace();
            System.out.println("[" + new Date() + "] Error creating server socket!");
            System.exit(103);
        }


    }


    synchronized private void sendMessage(JSONObject message)
    {
        for(String uname: OutMap.keySet())
        {
            addUserLog(uname,UUID.fromString(message.getString(MessageID)));
            try {
                OutMap.get(uname).writeObject(message.toString());
                OutMap.get(uname).flush();
            }catch(IOException iox)
            {
                //addUserLog(uname,UUID.fromString(message.getString(MessageID)));
            }

        }

    }

    private class ClientThread implements Runnable {

        User mUser;
        String mUserName;
        ObjectInputStream oin;
        boolean read;


        ClientThread(User user)
        {
            mUser = user;
            oin = mUser.reader;
            mUserName = mUser.name;
            read = true;


        }

        @Override
        public void run() {
            Thread.currentThread().setName(mUserName);
            while(read)
            {
                try{

                    Object object = oin.readObject();

                    if(object instanceof UUID)
                    {
                        UUID mID = (UUID)object;

                        //System.out.println("Received confirmation for message " + mID.toString());
                        clearUserLog(mUserName,mID);
                    }
                    else {
                        String msg = (String) object;
                        //System.out.println("Message from " + mUserName + " : " + msg);
                        if (msg.matches(logout_msg)) {
                            logoutUser(mUserName);
                        } else {
                            Date rcTime = new Date();

                            //String time = String.format("%tr", rcTime);
                            String time = TimeFormatter.format(rcTime);
                            String date = DateFormatter.format(rcTime);

                            UUID mID = UUID.randomUUID();
                            JSONObject msgObject = new JSONObject(msg);
                            msgObject.put(MessageID, mID.toString());
                            msgObject.put(MessageTime, time);
                            msgObject.put(MessageDate, date);
                            msgObject.put(MessageSender, mUserName);
                            msgObject.put(SenderHash, mUser.userHash);

                            addMessage(mID,rcTime,mUserName,mUser.userHash,msgObject.getString(MessageContent));
                            //addUserLog(mUserName,mID);

                            sendMessage(msgObject);

                        }

                    }
                } catch(IOException | ClassNotFoundException|JSONException ex){
                    //ex.printStackTrace();
                    //System.out.println("Breaking read connection with: " + mUserName);
                    break;}


            }
            //System.out.println("Stopping thread: " + Thread.currentThread().getName());

        }
    }


    private void cleanMessageTable()
    {


        //System.out.println("Performing message table cleanup!");

        try {

            ResultSet rs = dbStatement.executeQuery("select name from users");
            ArrayList<String> remList = new ArrayList<>();
            while(rs.next())
            {
                String uname = rs.getString(1);

                rs = dbStatement.executeQuery("select msgID from " + uname +" order by id desc limit 1");
                if(rs.first())
                {
                    String msgID = rs.getString(1);
                    GetMessageTime.setString(1,msgID);
                    rs = GetMessageTime.executeQuery();
                    rs.first();
                    Long upperTime = rs.getLong(1);

                    rs = dbStatement.executeQuery("select msgID from " + uname +" order by id limit 1");
                    rs.first();
                    msgID = rs.getString(1);
                    GetMessageTime.setString(1,msgID);
                    rs = GetMessageTime.executeQuery();
                    rs.first();
                    Long lowerTime = rs.getLong(1);
                    //System.out.println("Upper time: " + upperTime);
                    //System.out.println("Lower time: " + lowerTime);

                    if((upperTime - lowerTime) > LogoutTime)remList.add(uname);


                }



            }

            for(String user: remList)
            {
                logoutUser(user);

            }




            rs = dbStatement.executeQuery("select name from users");
            if(rs.first())
            {
                StringBuilder cmd = new StringBuilder("delete from messages where msgID not in (");
                do {
                    cmd.append("select msgID from " + rs.getString(1));
                    if(!rs.isLast())cmd.append(" union ");
                }while(rs.next());
                cmd.append(")");
                dbStatement.execute(cmd.toString());

            }
            else
            {
                dbStatement.execute("delete from messages where msgID is not null");
            }

        }catch(SQLException sqx){//sqx.printStackTrace();
            System.out.println("[" + new Date() + "] (207) Could not perform message-table cleanup!");}

    }



    private void addMessage(UUID msgID, Date timestamp, String username, String hash, String message)
    {
        try {

            InsertMessagesStatement.setString(1,msgID.toString());
            InsertMessagesStatement.setLong(2,timestamp.getTime());
            InsertMessagesStatement.setString(3,username);
            InsertMessagesStatement.setString(4,hash);
            InsertMessagesStatement.setString(5,message);
            InsertMessagesStatement.execute();

            /*
            dbStatement.execute("insert into messages values(\'" +
                    msgID.toString() + "\'," + timestamp.getTime() + ",\'" + username
                    + "\',\'" + hash + "\',\'" + message + "\');");*/
        } catch (SQLException sqx) {
            System.out.println("[" + new Date() + "] (201) Could not add " + msgID.toString() + " to message store!");
            //sqx.printStackTrace();
        }

    }

    private void tablesInit()
    {
        try {

            DatabaseMetaData dbm = dbConn.getMetaData();
            ResultSet rs = dbm.getTables(null, null, "users", new String[]{"TABLE"});
            if (rs.first()) {

                //System.out.println("Table \'users\' found!");
                rs = dbStatement.executeQuery("select * from users");

                while (rs.next()) {
                    User user = new User();
                    user.name = rs.getString(1);
                    user.userID = UUID.fromString(rs.getString(2));
                    user.loginHash = rs.getString(3);
                    byte[] hashbytes = MessageDigest.getInstance(HashAlgo).digest(user.loginHash.getBytes());
                    user.userHash = Base64.getEncoder().encodeToString(hashbytes);
                    UserList.put(user.name,user);

                }

                /*while (!rs.isClosed() && rs.next()) {

                    try {
                        dbStatement.execute("drop table " + rs.getString(1));
                    }catch(SQLException clrx){}

                }
                dbStatement.execute("delete from users where name is not null;");*/




            } else {
                //System.out.println("Creating table \'users\'.");
                dbStatement.execute("create table users(name varchar(10),UID varchar(36),hash varchar(50))");
            }
            rs = dbm.getTables(null, null, "messages", new String[]{"TABLE"});
            if (rs.first()) {
                //System.out.println("Table \'messages\' found!");
            } else {
                //System.out.println("Creating table \'messages\'.");
                dbStatement.execute("create table messages(msgID varchar(36),time long,sender varchar(10),hash varchar(44),msg varchar(400))");

            }
        }catch(SQLException|NoSuchAlgorithmException sqx){System.out.println("[" + new Date() + "] Error performing initialisation of tables!");
            //sqx.printStackTrace();
            System.exit(102);}

    }

    private void addUserLog(String username, UUID messageID)
    {
        try {

            PreparedStatement InsertUserLog = dbConn.prepareStatement("insert into "+ username +" values(0,?)");
            InsertUserLog.setString(1,messageID.toString());
            InsertUserLog.execute();

            //dbStatement.execute("insert into " + username + " values(\'"+ messageID.toString()+"\');");
        } catch (SQLException sqx) {
            System.out.println("[" + new Date() + "] (202) Could not add " + messageID.toString() +" to " + username +" store!");
            //sqx.printStackTrace();
        }

    }

    private void clearUserLog(String username, UUID messageID)
    {
        try{
            PreparedStatement ClearUserLog = dbConn.prepareStatement("delete from " + username + " where msgID like ?");
            ClearUserLog.setString(1,messageID.toString());
            ClearUserLog.execute();


            //dbStatement.execute("delete from " + username + " where msgID like \'" + messageID.toString() + "\';");
        }catch(SQLException sqx){System.out.println("[" + new Date() + "] (203) Could not perform confirmation for " + messageID.toString() +"!");
            //sqx.printStackTrace();
        }
    }




    private void clearUser(String username)
    {
        try {
            dbStatement.execute("drop table " + username);

            RemoveUser.setString(1,username);
            RemoveUser.execute();
            //dbStatement.execute("delete from users where name like \'" + username + "\'");
        }catch(SQLException sqx){System.out.println("[" + new Date() + "] (204) Could not clear tables for user " +username+"!");
            //sqx.printStackTrace();
        }

    }

    public synchronized void kickUser(String uname)
    {

            if(OutMap.containsKey(uname))
            {
                try {
                OutMap.get(uname).writeObject(logout_msg);
                }catch(IOException iox){}
            }

        logoutUser(uname);


    }


    public synchronized void logoutUser(String uname)
    {
        User remUser = UserList.get(uname);
        if(remUser.sessionID!=null)
        {
            ReadMap.get(remUser.sessionID).read = false;
            ReadMap.remove(remUser.sessionID);
            try {
                remUser.reader.close();
                remUser.writer.close();
            }catch(IOException iox){//iox.printStackTrace();
            }

        }
        OutMap.remove(uname);
        UserList.remove(uname);
        System.out.println("[" + new Date() + "] "+ uname + " has logged out!");
        clearUser(uname);

        JSONObject leaveMsg = new JSONObject();
        Date rcTime = new Date();

        //String time = String.format("%tr", rcTime);
        String time = TimeFormatter.format(rcTime);
        String date = DateFormatter.format(rcTime);

        UUID mID = UUID.randomUUID();
        leaveMsg.put(MessageID, mID.toString());
        leaveMsg.put(MessageTime, time);
        leaveMsg.put(MessageDate, date);
        leaveMsg.put(MessageSender, ServerName);
        leaveMsg.put(SenderHash, ServerHash);
        leaveMsg.put(MessageContent,uname+UserLogoutMsg);


        addMessage(mID,rcTime,ServerName,ServerHash,leaveMsg.getString(MessageContent));
        //addUserLog(mUserName,mID);

        sendMessage(leaveMsg);

    }

    private ResultSet getSyncMessages(String username)
    {
        ResultSet rs = null;
        try {

            rs = dbStatement.executeQuery("select messages.msgID,messages.sender," +
                    "messages.hash,messages.msg,messages.time from messages," + username
                    + " where messages.msgID=" + username + ".msgID;");

        }catch(SQLException sqx){System.out.println("[" + new Date() + "] (205) Could not fetch messages for user " + username + "!");
            //sqx.printStackTrace();
        }
        return rs;
    }

    private void createUser(String username, UUID userID, String userHash)
    {
        try {

            CreateUserEntry.setString(1,username);
            CreateUserEntry.setString(2,userID.toString());
            CreateUserEntry.setString(3,userHash);
            CreateUserEntry.execute();
            //dbStatement.execute("insert into users values(\'" +
            //        username + "\',\'" + userID + "\',\'" + userHash + "\')");

            //CreateUserTable.setNString(1,username);
            //CreateUserTable.execute();
            dbStatement.execute("create table " + username + "(id int auto_increment,msgID varchar(36), primary key(id));");
        }catch(SQLException sqx){System.out.println("[" + new Date() + "] (206) Could not add user " + username+ "!");
            //sqx.printStackTrace();
        }

    }


    class ServerThread implements Runnable {
        Socket sock;

        public ServerThread(Socket conntn) {
            sock = conntn;
        }

        @Override
        public void run() {
            try {
                //System.out.println(sock.getInetAddress());
                ObjectInputStream oin = new ObjectInputStream(sock.getInputStream());

                JSONObject logInfo = new JSONObject((String) oin.readObject());
                //System.out.println(logInfo.toString());
                ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

                if (UserList.size() > 7) {
                    out.writeObject(false);
                    out.flush();
                } else {

                    String uname = logInfo.getString(LoginName);


                    if (logInfo.has(LoginUUID)) {

                        UUID logID = UUID.fromString(logInfo.getString(LoginUUID));
                        String logHash = logInfo.getString(LoginHash);
                        //byte[] hashbytes = logInfo.getString(LoginHash).getBytes();
                        //String logHash = new String(hashbytes,"UTF-8");

                        if (UserList.containsKey(uname) && UserList.get(uname).userID.equals(logID)
                                && UserList.get(uname).loginHash.equals(logHash)) {
                            User exUser = UserList.get(uname);


                            if(exUser.sessionID!=null)
                            {ReadMap.get(exUser.sessionID).read = false;
                                ReadMap.remove(exUser.sessionID);
                                try {
                                    exUser.reader.close();
                                    exUser.writer.close();
                                }catch(IOException iox){}
                            }
                            //System.out.println("User " + exUser.name + " reconnecting! ");

                            exUser.reader = oin;
                            exUser.writer = out;
                            out.writeObject(true);
                            out.flush();



                            exUser.sessionID = UUID.randomUUID();

                            ClientThread readThread = new ClientThread(exUser);
                            Thread reader = new Thread(readThread);
                            reader.start();


                            ReadMap.put(exUser.sessionID, readThread);

                            OutMap.put(uname, out);
                            //System.out.println("Active threads: " + Thread.activeCount());


                            ResultSet rs = getSyncMessages(exUser.name);

                            if (rs != null) {
                                while (rs.next()) {

                                    Date rcTime = new Date(rs.getLong(5));
                                    String time = TimeFormatter.format(rcTime);
                                    String date = DateFormatter.format(rcTime);

                                    JSONObject msg = new JSONObject();
                                    msg.put(MessageID, rs.getString(1));
                                    msg.put(MessageSender, rs.getString(2));
                                    msg.put(SenderHash, rs.getString(3));
                                    msg.put(MessageContent, rs.getString(4));
                                    msg.put(MessageTime, time);
                                    msg.put(MessageDate, date);
                                    out.writeObject(msg.toString());
                                    out.flush();


                                }
                            }


                        } else {

                            /*System.out.println("Has username: " + UserList.containsKey(uname));
                            System.out.println("UUID match: " + UserList.get(uname).userID.equals(UUID.fromString(logInfo.getString(LoginUUID))));
                            System.out.println("Hash match: " + UserList.get(uname).loginHash.matches(logInfo.getString(LoginHash)));
                            System.out.println("Stored:\""+UserList.get(uname).loginHash+"\"");
                            System.out.println("Given :\""+logInfo.getString(LoginHash)+"\"");
                            System.out.println("Login Info mismatch. Rejecting.");*/

                            out.writeObject(false);
                            out.flush();
                        }
                    } else {

                        if(UserList.containsKey(logInfo.getString(LoginName)))
                        {
                            out.writeObject(false);
                            out.flush();
                        }
                        else{

                            User newUser = new User();
                            newUser.name = uname;
                            newUser.reader = oin;
                            newUser.writer = out;
                            newUser.userID = UUID.randomUUID();
                            //byte[] hashbytes = logInfo.getString(LoginHash).getBytes();
                            //String logHash = new String(hashbytes,"UTF-8");
                            String logHash = logInfo.getString(LoginHash);

                            newUser.loginHash = logHash;
                            byte[] uHash = MessageDigest.getInstance(HashAlgo).digest(logHash.getBytes());
                            newUser.userHash = Base64.getEncoder().encodeToString(uHash);

                            newUser.sessionID = UUID.randomUUID();
                            out.writeObject(true);
                            out.writeObject(newUser.userID);
                            out.flush();

                            OutMap.put(uname, out);
                            UserList.put(uname, newUser);
                            System.out.println("[" + new Date() + "] " + newUser.name + " has logged in!");
                            ClientThread readThread = new ClientThread(newUser);
                            new Thread(readThread).start();
                            ReadMap.put(newUser.sessionID, readThread);
                            createUser(newUser.name, newUser.userID, newUser.loginHash);



                            JSONObject joinMsg = new JSONObject();
                            Date rcTime = new Date();

                            //String time = String.format("%tr", rcTime);
                            String time = TimeFormatter.format(rcTime);
                            String date = DateFormatter.format(rcTime);

                            UUID mID = UUID.randomUUID();
                            joinMsg.put(MessageID, mID.toString());
                            joinMsg.put(MessageTime, time);
                            joinMsg.put(MessageDate, date);
                            joinMsg.put(MessageSender, ServerName);
                            joinMsg.put(SenderHash, ServerHash);
                            joinMsg.put(MessageContent,uname+UserLoginMsg);


                            addMessage(mID,rcTime,ServerName,ServerHash,joinMsg.getString(MessageContent));
                            //addUserLog(mUserName,mID);

                            sendMessage(joinMsg);
                        }


                    }
                }
            }catch(IOException | NoSuchAlgorithmException | SQLException | ClassNotFoundException| JSONException ex){
                System.out.println("[" + new Date() + "] (301) Login error!");
                //ex.printStackTrace();
            }

        }

    }


    class AcceptConnections implements Runnable
    {

        @Override
        public void run() {

            System.out.println("[" + new Date() + "] Enclave Server Running!");

            while(true)
            {
                try {

                    Socket sock = serverSocket.accept();


                    new Thread(new ServerThread(sock)).start();



                }catch(IOException x)
                {
                    System.out.println("[" + new Date() + "] (104) Error binding to client!");
                    //x.printStackTrace();

                }



            }


        }
    }


    public static void main(String[] args) {

        //System.out.println(UUID.randomUUID());

        if(args.length != 3)System.out.println("Usage: java -jar Enclave.jar database_address keystore_filename port_number");
        else
        {
            Console console = System.console();
            String uname = console.readLine("Enter username for database: ");
            char[] dbpass = console.readPassword("Enter password for database: ");
            char[] keypass = console.readPassword("Enter password for keystore file: ");


            new EnclaveServer(args[0],uname,new String(dbpass),args[1],new String(keypass),args[2]).go();

        }


    }


    public void go()
    {
        Scanner cmdScanner = new Scanner(System.in);
        new Thread(new AcceptConnections()).start();
        while(true)
        {

            String cmd = cmdScanner.nextLine();
            String[] params = cmd.split(" ");
            if(params[0].equals("kick"))
            {
                if(UserList.containsKey(params[1]))
                {
                    kickUser(params[1]);
                }
                else
                {
                    System.out.println("Invalid username!");
                }
            }
            else
            {
                System.out.println("Invalid command!");
            }


        }



    }
}
