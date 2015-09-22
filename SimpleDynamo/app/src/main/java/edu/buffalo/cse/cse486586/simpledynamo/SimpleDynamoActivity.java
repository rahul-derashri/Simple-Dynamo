package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));



        Button LDump = (Button)findViewById(R.id.button1);

        LDump.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                TextView v1 = (TextView)findViewById(R.id.textView1);
                Cursor cur = getContentResolver().query(SimpleDynamoProvider.providerUri, null, "@", null, null);
                if( cur != null ){
                    while(cur.moveToNext()){
                        String key = cur.getString(0);
                        String val = cur.getString(1);
                        v1.append(key+"-->"+val+"\n\n");
                    }
                }
            }
        });



        Button GDump = (Button)findViewById(R.id.button2);

        GDump.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                new GlobalDump().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            }
        });
	}


    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class GlobalDump extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            //try {
            Cursor cur = getContentResolver().query(SimpleDynamoProvider.providerUri, null, "*", null, null);
            if( cur != null ){
                while(cur.moveToNext()){
                    String key = cur.getString(0);
                    String val = cur.getString(1);
                    publishProgress(key,val);

                }
            }
            return null;
        }


        protected void onProgressUpdate(String... values) {

            TextView v = (TextView)findViewById(R.id.textView1);
            v.append(values[0]+"-->"+values[1]+"\n\n");
        }
    }

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
