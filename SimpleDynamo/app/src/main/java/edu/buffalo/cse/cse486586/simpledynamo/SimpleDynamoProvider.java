package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteCantOpenDatabaseException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

public class SimpleDynamoProvider extends ContentProvider {

    private String[] REMOTE_PORTS = { "11108" , "11112" , "11116" , "11120" , "11124"};
    static final int SERVER_PORT = 10000;
    public static String myPort = null;

    public int selfIndex = 0;
    private TreeMap<String,String> ringNodes = new TreeMap<>();

    private SQLiteDatabase database;
    private static int DATABASE_VERSION = 1;
    private static final String DATABASE_NAME = "SimpleDynamo.db";
    private static final String TABLE_NAME = "MESSAGES";
    private static final String MESSAGE_TABLE = "CREATE TABLE " + TABLE_NAME + " (key TEXT PRIMARY KEY, value TEXT NOT NULL , version NUMBER);";

    public static final Uri providerUri = Uri.parse( Constants.URI);

    private boolean isReplyReceived = false;
    private boolean isDhtReplyReceived = false;
    private String[][] results;
    private Map<String , String[][]> resultMap = new HashMap<>();
    private List<String[]> dataList = new LinkedList();
    private int resultCount = 0;
    private boolean isRingFormed = false;
    private Map<String , Boolean> queryReplies = new HashMap<>();
    private Map<String , Integer> deleteReplies = new HashMap<>();
    private Map<String , Integer> insertReplies = new HashMap<>();
    private int recoveryReplies = 0;

    private boolean isFailureDetected = false;
    private boolean isMessagingStarted = false;
    private boolean isFailureHandled = false;
    private String failedPort = null;
    private boolean isRecovering = false;
    private Map<String , Long> lastSeen = new HashMap<>();

    private JSONObject mappings = new JSONObject();
    private List<String> heartBeats = new LinkedList<>();

    private class DBHelper extends SQLiteOpenHelper {
        public DBHelper(Context context){
            super(context, DATABASE_NAME, null , DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(MESSAGE_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.v("DBHelper" ,"OnUpgrade" );
            db.execSQL("DROP TABLE MESSAGES;");
            db.execSQL(MESSAGE_TABLE);
        }
    }

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

        int noOfRowDeleted = 0;

        try{
            if( selection.contains("*") ){
                resultCount = 0;
                resultCount = database.delete(TABLE_NAME,null,null);

                Message msg = new Message(myPort, null , Constants.DHT_DELETE_REQUEST , null);
                msg.setKey(selection);

                unicast(msg, mappings.getJSONObject(myPort).get("successor1").toString());

                noOfRowDeleted = resultCount;
            }
            else if( selection.contains("@") ){
                noOfRowDeleted = database.delete(TABLE_NAME,null,null);
            }
            else{
                String receiver = findReceiver(selection);
                String hashKey = genHash(selection);

                if( receiver.equalsIgnoreCase(myPort) ){

                    noOfRowDeleted = myDelete(selection);
                    Message msg = new Message(myPort, null , Constants.DELETE_REQUEST , hashKey);
                    msg.setKey(selection);
                    msg.setReplicationsCount(1);
                    deleteReplies.put(selection , 0);

                    unicast(msg, mappings.getJSONObject(myPort).get("successor1").toString());
                    unicast(msg, mappings.getJSONObject(myPort).get("successor2").toString());

                }
                else {

                    Message msg = new Message(myPort, null , Constants.DELETE_REQUEST , hashKey);
                    msg.setKey(selection);
                    deleteReplies.put(selection , 0);
                    unicast(msg, receiver);
                    unicast(msg, mappings.getJSONObject(receiver).get("successor1").toString());
                    unicast(msg, mappings.getJSONObject(receiver).get("successor2").toString());

                    noOfRowDeleted = resultCount;
                    resultCount = 0;

                }
            }
        }
        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
            Log.e("NoSuchAlgorithmException" , "delete method");
        }
        catch (Exception e){
            e.printStackTrace();
            Log.e("Exception in deleting " ,"delete method");
        }

		return noOfRowDeleted;
	}

    public synchronized int myDelete(String selection){
        int noOfRowDeleted = database.delete(TABLE_NAME,"key='"+selection+"'",null);
        return noOfRowDeleted;
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

    public synchronized int getVersion(String key){
        Cursor cur = myQuery(key , "DB");
        if( cur != null && cur.moveToFirst()){
            return cur.getInt(2);
        }

        return 0;
    }

	@Override
	public Uri insert(Uri uri, ContentValues values) {


        try{

            if( isRingFormed ){
                String key = (String) values.get("key");
                String hashKey = genHash(key);
                String value = (String) values.get("value");
                String receiver = findReceiver(key);

                if( myPort.equalsIgnoreCase(receiver) ){

                    int version = getVersion(key)+1;
                    values.put("version" , version);
                    myInsert(values);

                    Message msg = new Message(myPort, null, Constants.INSERT_REPLICATION_REQUEST , hashKey);
                    msg.setKey(key);
                    msg.setValue(value);
                    msg.setVersion(version);

                    insertReplies.put(key , 0);
                    unicast(msg, mappings.getJSONObject(myPort).get("successor1").toString());

                    unicast(msg, mappings.getJSONObject(myPort).get("successor2").toString());

                    while (insertReplies.get(key) < 1){

                    }
                }
                else{
                    //Log.v("else block" , "insert method");
                    Message msg = new Message(myPort, null, Constants.INSERT_REQUEST , hashKey);
                    msg.setKey(key);
                    msg.setValue(value);
                    insertReplies.put(key, 0);
                    unicast(msg, receiver);

                    unicast(msg, mappings.getJSONObject(receiver).get("successor1").toString());

                    unicast(msg, mappings.getJSONObject(receiver).get("successor2").toString());

                    while (insertReplies.get(key) < 2){

                    }

                    isReplyReceived = false;
                }
            }
            else{

                myInsert(values);
            }
        }
        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
            Log.e("NoSuchAlgorithmException", "insert method");
        }
        catch (Exception e){
            e.printStackTrace();
            Log.e("Exception in inserting to proper node", "insert method");
        }

		return uri;
	}


    public synchronized Uri myInsert(ContentValues values){
        long ID = database.insertWithOnConflict(TABLE_NAME , null, values , SQLiteDatabase.CONFLICT_REPLACE);

        if (ID > 0) {
            Uri uri1 = ContentUris.withAppendedId(providerUri, ID);
            // For updation
            getContext().getContentResolver().notifyChange(uri1, null);
            return uri1;
        }
        else{
            Log.v("myInsert" , "Insert Failed");
        }

        return null;
    }

	@Override
	public boolean onCreate() {

        if( isDatabaseExists() ){
            isRecovering = true;
            DATABASE_VERSION++;
        }

        DBHelper helper = new DBHelper(getContext());
        database = helper.getWritableDatabase();
        boolean status = false;
        if( database != null )
            status = true;
        else
            status = false;

        createClientAndServer();


        return status;
	}

    public boolean isDatabaseExists(){
        try{
            SQLiteDatabase dataB = SQLiteDatabase.openDatabase(Constants.DB_PATH+DATABASE_NAME , null , SQLiteDatabase.OPEN_READONLY);
            dataB.close();
            return true;
        }
        catch (SQLiteCantOpenDatabaseException e){
            return false;
        }
        catch (Exception e){
            return false;
        }

    }

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
        builder.setTables(TABLE_NAME);
        Cursor cursor = null;
        String[] colNames = {"key" , "value"};
        String[] cols = {"key" , "value"};

        try{

            if( selection == null ){
                cursor = builder.query(database,cols,null,null,null,null,null);
            }
            else{
                if( selection.contains("*") ){
                    dataList = new LinkedList<>();
                    cursor = getContext().getContentResolver().query(providerUri , null, null,null , null);
                    if( cursor != null ){
                        cursor.moveToFirst();
                        while(cursor.moveToNext()){
                            String key = cursor.getString(0);
                            String val = cursor.getString(1);

                            String[] row = {key,val/*,version*/};
                            dataList.add(row);
                        }
                    }

                    Message msg = new Message(myPort, null ,Constants.DHT_QUERY_REQUEST , null);
                    msg.setKey(selection);

                    unicast(msg, mappings.getJSONObject(myPort).get("successor1").toString());

                    while (!isDhtReplyReceived){

                    }


                    isDhtReplyReceived = false;

                    MatrixCursor mCursor = new MatrixCursor(colNames);

                    ListIterator<String[]> it = dataList.listIterator();
                    while (it.hasNext()){
                        mCursor.addRow((String[])it.next());
                    }

                    results = null;
                    return mCursor;
                }
                else if(selection.contains("@")){

                    cursor = builder.query(database,cols,null,null,null,null,null);
                }
                else{
                    String hashKey = genHash(selection);
                    String receiver = findReceiver(selection);

                    if( receiver.equalsIgnoreCase(myPort) ){
                        cursor = myQuery(selection , null);

                    }
                    else {


                            Message msg = new Message(myPort, null ,Constants.QUERY_REQUEST , hashKey);
                            msg.setKey(selection);
                            queryReplies.put(selection , false);
                            unicast(msg, receiver);
                            unicast(msg, mappings.getJSONObject(receiver).get("successor1").toString());
                            while (!queryReplies.get(selection) ){

                            }

                            isReplyReceived = false;

                            MatrixCursor mCursor = new MatrixCursor(colNames);

                            if( resultMap.containsKey(selection)){
                                String[][] rs = resultMap.get(selection);
                                for(int counter = 0; counter < rs.length; counter++){
                                    mCursor.addRow(rs[counter]);
                                }

                                resultMap.remove(selection);
                            }


                            return mCursor;

                    }
                }
            }


        }
        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
            Log.e("NoSuchAlgorithmException" , "insert method");
        }
        catch (Exception e){
            e.printStackTrace();
            Log.e("Exception in inserting to proper node" ,"insert method");
        }

		return cursor;
	}

    public synchronized Cursor myQuery(String selection ,String val){
        Cursor cursor = null;
        if( val!=null )
            cursor = database.rawQuery("SELECT * FROM MESSAGES WHERE key = '"+selection+"'" , null);
        else
            cursor = database.rawQuery("SELECT key, value FROM MESSAGES WHERE key = '"+selection+"'" , null);

        return cursor;
    }


    public synchronized Cursor insertOrQuery(String type , Object data){
        switch (type){
            case Constants.INSERT_R:
                myInsert((ContentValues)data);
                break;
            case Constants.INSERT:
                ContentValues values = (ContentValues)data;
                int lVersion = getVersion((String)values.get("key"));
                values.put("version" , lVersion+1);
                myInsert(values);
                break;
            case Constants.BULK_INSERT:
                ContentValues keyValueToInsert1 = new ContentValues();
                String[][] rs = (String[][])data;
                for( int counter = 0;counter < rs.length ; counter++ ){
                    String key = rs[counter][0];
                    if( isBelongToMe(key) ){
                        keyValueToInsert1 = new ContentValues();
                        keyValueToInsert1.put("key",key);
                        keyValueToInsert1.put("value",rs[counter][1]);
                        int lVersion1 = getVersion(key);
                        int version = Integer.parseInt(rs[counter][2]);
                        if( version > lVersion1 ){
                            keyValueToInsert1.put("version" , version);
                            myInsert(keyValueToInsert1);
                        }
                    }
                }
                break;
            case Constants.QUERY:
                return myQuery((String)data , null);
        }

        return null;
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    private void createClientAndServer(){
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e("Activity", "Can't create a ServerSocket");
            return;
        }

        handleJoinRequest();
        isRingFormed = true;


        Timer timer = new Timer();
        timer.schedule(new HeartBeat() , 100 , 100);

        if( isRecovering ){
            new RecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR , null , myPort);
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, null, myPort);


    }



    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     *
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     *
     * @author stevko
     *
     */
    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

            while(true){
                try{

                    ObjectInputStream stream = new ObjectInputStream(serverSocket.accept().getInputStream());
                    Message message = (Message)stream.readObject();

                    String typeOfMessage = message.getType();

                    lastSeen.put(message.getSenderNodeId() , System.currentTimeMillis());

                    if( !isMessagingStarted && typeOfMessage!= null ){
                        isMessagingStarted = true;
                        Log.v("type Of 1st message" , typeOfMessage);
                    }


                    switch (typeOfMessage){

                        case Constants.RECOVERY_UPDATE:

                            isFailureDetected = false;
                            isFailureHandled = false;
                            failedPort = null;

                            heartBeats = new LinkedList<>();
                            break;

                        case Constants.RECOVERY_REQUEST:

                            isFailureDetected = false;
                            isFailureHandled = false;
                            failedPort = null;

                            heartBeats = new LinkedList<>();

                            Cursor cur2 = database.rawQuery("SELECT * FROM MESSAGES;" , null);

                            if( cur2 != null ){
                                int noOfResults = cur2.getCount();
                                String[][] results = new String[noOfResults][3];
                                int count = -1;

                                cur2.moveToFirst();
                                while(count < noOfResults-1 ){
                                    count++;
                                    String key = cur2.getString(0);
                                    String val = cur2.getString(1);
                                    String version = cur2.getString(2);
                                    results[count][0] = key;
                                    results[count][1] = val;
                                    results[count][2] = version;

                                    cur2.moveToNext();
                                }

                                message.setMessage(results);
                                message.setType(Constants.RECOVERY_REPLY);
                                String port = message.getSenderNodeId();
                                message.setSenderNodeId(myPort);

                                unicast(message, port);
                            }
                            break;
                        case Constants.RECOVERY_REPLY:

                            recoveryReplies++;

                            String[][] rs = (String[][])message.getMessage();

                            insertOrQuery(Constants.BULK_INSERT , rs);

                            break;
                        case Constants.HEART_BEATS:
                            lastSeen.put(message.getSenderNodeId() , System.currentTimeMillis());

                            break;
                        case Constants.INSERT_REPLY:

                            String key1 = message.getKey();

                            synchronizeOperations(typeOfMessage , key1);
                            isReplyReceived = true;
                            break;
                        case Constants.INSERT_REQUEST:

                            ContentValues keyValueToInsert = new ContentValues();
                            keyValueToInsert.put("key",message.getKey());
                            keyValueToInsert.put("value",message.getValue());

                            insertOrQuery(Constants.INSERT , keyValueToInsert);

                            message.setType(Constants.INSERT_REPLY);
                            unicast(message, message.getSenderNodeId());

                            break;
                        case Constants.INSERT_REPLICATION_REQUEST:

                            ContentValues keyValueToInsert2 = new ContentValues();
                            keyValueToInsert2.put("key",message.getKey());
                            keyValueToInsert2.put("value",message.getValue());
                            keyValueToInsert2.put("version" , message.getVersion());

                            insertOrQuery(Constants.INSERT_R , keyValueToInsert2);

                            message.setType(Constants.INSERT_REPLY);
                            unicast(message, message.getSenderNodeId());

                            break;
                        case Constants.QUERY_REPLY:
                            resultMap.put(message.getKey() , (String[][])message.getMessage());

                            synchronizeOperations(typeOfMessage , message.getKey());

                            isReplyReceived = true;
                            break;
                        case Constants.QUERY_REQUEST:
                            String selection = message.getKey();

                            Cursor cur = insertOrQuery(Constants.QUERY , selection);

                            if( cur != null ){
                                String[][] results = new String[cur.getCount()][2];
                                if(cur.moveToFirst()){
                                    String key = cur.getString(0);
                                    String val = cur.getString(1);

                                    results[0][0] = key;
                                    results[0][1] = val;

                                    message.setMessage(results);
                                    message.setType(Constants.QUERY_REPLY);
                                    unicast(message, message.getSenderNodeId());
                                }
                            }

                            break;
                        case Constants.DHT_QUERY_REPLY:

                            List list = (LinkedList)message.getMessage();
                            dataList.addAll(list);
                            isDhtReplyReceived = true;
                            break;

                        case Constants.DHT_QUERY_REQUEST:

                            String[] cols = {"key" , "value"};
                            Cursor cur1 = database.query(TABLE_NAME , cols , null, null, null, null, null);
                            if( cur1 != null ){
                                List<String[]> tempdataList = new LinkedList();

                                while(cur1.moveToNext()){
                                    String key = cur1.getString(0);
                                    String val = cur1.getString(1);

                                    String[] row = {key,val/*,version*/};

                                    tempdataList.add(row);
                                }

                                if( message.getMessage() != null ){
                                    ((LinkedList)message.getMessage()).addAll(tempdataList);
                                }
                                else {
                                    message.setMessage(tempdataList);
                                }

                                if( mappings.getJSONObject(myPort).get("successor1").toString().equalsIgnoreCase(message.getSenderNodeId()) ){
                                    message.setType(Constants.DHT_QUERY_REPLY);
                                }

                                unicast(message, mappings.getJSONObject(myPort).get("successor1").toString());

                            }
                            break;
                        case Constants.DELETE_REPLY:

                            deleteReplies.put(message.getKey() , 1);
                            isReplyReceived = true;
                            break;
                        case Constants.DELETE_REQUEST:

                            resultCount = 0;

                            String selection1 = message.getKey();

                            myDelete(selection1);

                            break;
                        case Constants.DHT_DELETE_REPLY:


                            resultCount += (Integer)message.getMessage();

                            isReplyReceived = true;
                            break;
                        case Constants.DHT_DELETE_REQUEST:

                            resultCount = database.delete(TABLE_NAME , null , null);


                            if( mappings.getJSONObject(myPort).get("successor1").toString().equalsIgnoreCase(message.getSenderNodeId()) ){
                                message.setType(Constants.DHT_DELETE_REPLY);
                            }

                            unicast(message, mappings.getJSONObject(myPort).get("successor1").toString());

                            break;
                    }

                }
                catch (ClassNotFoundException e){
                    Log.e("Server" , "Message class not found");
                }
                catch (SocketTimeoutException e){

                    if( isMessagingStarted && !isFailureHandled) {

                        sendHeartBeats();
                        isFailureHandled = true;
                    }

                }
                catch(IOException e){
                    e.printStackTrace();
                    Log.e("Server", "Error in reading message");
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }

        }

        protected void onProgressUpdate(Message...msgs) {
            /*
             * The following code displays what is received in doInBackground().
             */
            Message msg = msgs[0];

            ContentValues keyValueToInsert = new ContentValues();
            keyValueToInsert.put("key",msg.getKey());
            keyValueToInsert.put("value",msg.getValue());

            Uri newUri = getContext().getContentResolver().insert(
                    SimpleDynamoProvider.providerUri,
                    keyValueToInsert
            );

            return;
        }
    }


    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            Socket socket = null;
            ObjectOutputStream stream = null;

            return null;
        }
    }

    private class RecoveryTask extends AsyncTask<String ,Void , Void >{
        @Override
        protected Void doInBackground(String... params) {
            try {
                recoveryReplies = 0;

                lastSeen = new HashMap<>();

                String succ = mappings.getJSONObject(myPort).get("successor1").toString();
                String pred = mappings.getJSONObject(myPort).get("predecessor1").toString();
                Message message = new Message(myPort , null , Constants.RECOVERY_REQUEST , null);

                String recoveryPorts[] = {
                        succ ,

                        mappings.getJSONObject(myPort).get("predecessor2").toString() ,
                        mappings.getJSONObject(myPort).get("predecessor1").toString()};


                Socket socket = null;
                ObjectOutputStream stream = null;

                for( String port : recoveryPorts){

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));

                    stream = new ObjectOutputStream( new BufferedOutputStream(socket.getOutputStream()));
                    stream.writeObject(message);
                    stream.flush();
                    stream.close();
                    socket.close();
                }

            }
            catch (JSONException e){
                e.printStackTrace();
                Log.e("RecoveryTask", "JSONException");
            }
            catch (UnknownHostException e) {
                Log.e("RecoveryTask", "UnknownHostException");
            }
            catch (SocketException e){
                Log.e("RecoveryTask", "Socket Exception for port ");
                e.printStackTrace();
            }
            catch (SocketTimeoutException e) {
                Log.e("RecoveryTask", "Socket Timeout for port ");
            }
            catch (IOException e) {
                e.printStackTrace();
                Log.e("RecoveryTask", "socket IOException");
            }
            catch (Exception e){
                //e.printStackTrace();
            }

            return null;
        }
    }

    /*private class insertPktTask extends AsyncTask<String ,Void , Void >{
        @Override
        protected Void doInBackground(String... params) {

            
            return null;
        }
    }*/


    public void unicast(Message message , String port){
        try {

            Socket socket = null;
            ObjectOutputStream stream = null;
            /*
            * Looping to send the message to all the AVD's. There is no need to identify remote ports
            * because we have to send it to all AVD's including the client.
            */


            if( isFailureDetected && port.equalsIgnoreCase(failedPort)){

                String type = message.getType();
                String succ = mappings.getJSONObject(port).get("successor1").toString();
                switch (type){
                    case Constants.DHT_QUERY_REQUEST:

                        if( succ.equalsIgnoreCase(message.getSenderNodeId()) ){
                            message.setType(Constants.DHT_QUERY_REPLY);
                        }
                        port = succ;

                        break;
                    case Constants.DHT_DELETE_REQUEST:

                        if( succ.equalsIgnoreCase(message.getSenderNodeId()) ){
                            message.setType(Constants.DHT_DELETE_REPLY);
                        }
                        port = succ;
                        break;


                }
            }

            if( message.type != null ){
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));

                stream = new ObjectOutputStream( new BufferedOutputStream(socket.getOutputStream()));
                stream.writeObject(message);
                stream.flush();
                stream.close();
                socket.close();
            }

        } catch (UnknownHostException e) {
            Log.e("Client", "ClientTask UnknownHostException");
        }
        catch (SocketException e){
            Log.e("Client", "ClientTask socket Exception for port "+port);
            e.printStackTrace();
        }
        catch (SocketTimeoutException e) {
            Log.e("Client", "ClientTask socket Timeout for port "+port);
        }
        catch (IOException e) {
            e.printStackTrace();
            Log.e("Client", "ClientTask socket IOException");
        }
        catch (Exception e){
            //e.printStackTrace();
        }

    }

    public synchronized void synchronizeOperations(String typeOfMessage ,  String key){
        switch (typeOfMessage){
            case Constants.QUERY_REPLY:
                queryReplies.put(key , true);
                break;
            case Constants.INSERT_REPLY:
                insertReplies.put(key , insertReplies.get(key) +1);
                break;
        }
    }


    public String getNodeId(String port){
        String nodeId = "";
        switch (port){
            case "11108":
                nodeId = "5554";
                break;
            case "11112":
                nodeId = "5556";
                break;
            case "11116":
                nodeId = "5558";
                break;
            case "11120":
                nodeId = "5560";
                break;
            case "11124":
                nodeId = "5562";
                break;

        }
        return nodeId;
    }


    public void handleJoinRequest(){
        try{

            try{
                for(int counter=0;counter<REMOTE_PORTS.length;counter++){
                    ringNodes.put(genHash(getNodeId(REMOTE_PORTS[counter])) , REMOTE_PORTS[counter]);
                }
            }
            catch (NoSuchAlgorithmException e){
                e.printStackTrace();
            }


            Iterator it = ringNodes.keySet().iterator();
            REMOTE_PORTS = new String[5];

            int i = 0;
            REMOTE_PORTS[0] = (String)ringNodes.get((String)it.next());
            if( REMOTE_PORTS[0].equalsIgnoreCase(myPort) ){
                selfIndex = 0;
            }

            while (it.hasNext()){
                i++;
                String port = (String)ringNodes.get((String)it.next());

                REMOTE_PORTS[i] = port;

                if( port.equalsIgnoreCase(myPort) ){
                    selfIndex = i;

                }

            }

            generateMappings();


        }
        catch (Exception e){
            e.printStackTrace();
            Log.e("Exception" , "handleJoinRequest()");
        }

    }


    public void generateMappings(){
        try{
            for( int counter = 0; counter < REMOTE_PORTS.length ; counter++ ){
                JSONObject obj = new JSONObject();
                switch (counter){
                    case 3:
                        obj.put("successor1" , REMOTE_PORTS[counter+1]);
                        obj.put("successor2" , REMOTE_PORTS[0]);
                        obj.put("predecessor1" , REMOTE_PORTS[counter-1]);
                        obj.put("predecessor2" , REMOTE_PORTS[counter-2]);
                        break;
                    case 4:
                        obj.put("successor1" , REMOTE_PORTS[0]);
                        obj.put("successor2" , REMOTE_PORTS[1]);
                        obj.put("predecessor1" , REMOTE_PORTS[counter-1]);
                        obj.put("predecessor2" , REMOTE_PORTS[counter-2]);
                        break;
                    case 0:
                        obj.put("successor1" , REMOTE_PORTS[counter+1]);
                        obj.put("successor2" , REMOTE_PORTS[counter+2]);
                        obj.put("predecessor1" , REMOTE_PORTS[4]);
                        obj.put("predecessor2" , REMOTE_PORTS[3]);
                        break;
                    case 1:
                        obj.put("successor1" , REMOTE_PORTS[counter+1]);
                        obj.put("successor2" , REMOTE_PORTS[counter+2]);
                        obj.put("predecessor1" , REMOTE_PORTS[0]);
                        obj.put("predecessor2" , REMOTE_PORTS[4]);
                        break;
                    default:
                        obj.put("successor1" , REMOTE_PORTS[counter+1]);
                        obj.put("successor2" , REMOTE_PORTS[counter+2]);
                        obj.put("predecessor1" , REMOTE_PORTS[counter-1]);
                        obj.put("predecessor2" , REMOTE_PORTS[counter-2]);
                        break;
                }

                mappings.put(REMOTE_PORTS[counter] , obj);
            }

        }
        catch (JSONException e){
            e.printStackTrace();
        }
    }


    public String findReceiver(String key){
        try{
            String hashKey = genHash(key);

            for(int counter = 0;counter < REMOTE_PORTS.length; counter++){
                String hashNodekey = genHash(getNodeId(REMOTE_PORTS[counter]));
                String hashPredecessorNodeKey = null;

                if( counter == 0 )
                    hashPredecessorNodeKey = genHash(getNodeId(REMOTE_PORTS[REMOTE_PORTS.length-1]));
                else
                    hashPredecessorNodeKey = genHash(getNodeId(REMOTE_PORTS[counter-1]));

                if( counter == 0 ){
                    if( hashKey.compareTo(hashPredecessorNodeKey) > 0 || hashKey.compareTo(hashNodekey) <= 0){

                        return REMOTE_PORTS[counter];
                    }
                }
                else{
                    if( hashKey.compareTo(hashNodekey) <= 0 && hashKey.compareTo(hashPredecessorNodeKey) > 0  ){

                        return REMOTE_PORTS[counter];
                    }
                }
            }

        }
        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }

        return null;

    }

    public void sendHeartBeats(){
        Message msg = new Message(myPort , null , Constants.HEART_BEATS , null);
        for( int counter = 0; counter< REMOTE_PORTS.length ; counter++ ){
            if( !REMOTE_PORTS[counter].equalsIgnoreCase(myPort)){
                unicast(msg , REMOTE_PORTS[counter]);
            }
        }
    }


    public boolean isBelongToMe(String key){
        try{
            String hashKey = genHash(key);
            String port = myPort;
            String hashNodekey = genHash(getNodeId(port));
            String hashPredecessorNodeKey = genHash(getNodeId(mappings.getJSONObject(port).get("predecessor1").toString()));
            String smallestNode = REMOTE_PORTS[0];
            if( hashKey.compareTo(hashNodekey) <= 0 && hashKey.compareTo(hashPredecessorNodeKey) > 0  ){
                return true;
            }
            else if( port.equalsIgnoreCase(smallestNode) && ( hashKey.compareTo(hashPredecessorNodeKey) > 0 || hashKey.compareTo(hashNodekey) <= 0)){
                return true;
            }


            port = mappings.getJSONObject(port).get("predecessor1").toString();
            hashNodekey = genHash(getNodeId(port));
            hashPredecessorNodeKey = genHash(getNodeId(mappings.getJSONObject(port).get("predecessor1").toString()));
            if( hashKey.compareTo(hashNodekey) <= 0 && hashKey.compareTo(hashPredecessorNodeKey) > 0  ){
                return true;
            }
            else if( port.equalsIgnoreCase(smallestNode) && ( hashKey.compareTo(hashPredecessorNodeKey) > 0 || hashKey.compareTo(hashNodekey) <= 0)){
                return true;
            }

            port = mappings.getJSONObject(port).get("predecessor1").toString();
            hashNodekey = genHash(getNodeId(port));
            hashPredecessorNodeKey = genHash(getNodeId(mappings.getJSONObject(port).get("predecessor1").toString()));
            if( hashKey.compareTo(hashNodekey) <= 0 && hashKey.compareTo(hashPredecessorNodeKey) > 0  ){
                return true;
            }
            else if( port.equalsIgnoreCase(smallestNode) && ( hashKey.compareTo(hashPredecessorNodeKey) > 0 || hashKey.compareTo(hashNodekey) <= 0)){
                return true;
            }


        }
        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        catch (JSONException e){
            e.printStackTrace();
        }
        return false;
    }

    class HeartBeat extends TimerTask{
        @Override
        public void run() {
            try{
                Message message = new Message(myPort , null , Constants.HEART_BEATS , null);

                unicast(message , mappings.getJSONObject(myPort).get("predecessor1").toString());
            }
            catch (JSONException e){
                e.printStackTrace();
            }


            try{
                if( failedPort == null ) {
                    String port = mappings.getJSONObject(myPort).get("successor1").toString();
                    if (lastSeen.containsKey(port) && (System.currentTimeMillis() - lastSeen.get(port)) > 1000) {
                        failedPort = port;
                        Log.v("TimerTask", "failed Port detected : " + failedPort);
                        isFailureHandled = true;
                        isFailureDetected = true;
                    }
                }
            }
            catch (JSONException e){
                e.printStackTrace();
            }

        }
    }

}
