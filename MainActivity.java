package com.example.gps;

import android.Manifest;
import android.location.Location;
import android.os.Bundle;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;

import android.os.AsyncTask;
import android.util.Log;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.WriteChannel;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.WriteChannelConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;


public class MainActivity extends AppCompatActivity {

    private String datasetName = "test_tabel";
    private String JsonRows = "";
    private String csv_content;
    private int num_rows = 1;

    private String distanceQuery = "SELECT ROUND(ST_LENGTH(ST_MAKELINE(arr)), 1)as length from (SELECT array_agg(g) as arr FROM " +
            "(SELECT *, ST_GeogPoint(lon, lat) AS g FROM `cleveronak.test_tabel.geo_points`" +
            " ORDER BY id))";

    private String deleteQuery = "truncate table `cleveronak.test_tabel.geo_points` ";


    private TextView distanceText;
    private Button btnGetLoc;
    private Button btnDist;
    private Button btnDelData;

    private FieldValue distance;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        setContentView(R.layout.activity_main);
        btnGetLoc = findViewById(R.id.btnGetLoc);
        btnDist = findViewById(R.id.btnDist);
        btnDelData = findViewById(R.id.btnDelData);
        distanceText = findViewById(R.id.distText);
        ActivityCompat.requestPermissions(MainActivity.this, new String[]{Manifest.permission.ACCESS_FINE_LOCATION}, 123); // запрос разрешение на использовние геопозиции
        ActivityCompat.requestPermissions(MainActivity.this, new String[]{Manifest.permission.INTERNET}, 123); // запрос разрешение на использовние геопозиции

        btnGetLoc.setOnClickListener(v -> {
            GPStracker g = new GPStracker(getApplicationContext());
            Location l = g.getLocation();

            if (l != null) {

                csv_content = num_rows + "," + l.getLatitude() + "," + l.getLongitude();
                new BigQueryTask().execute(JsonRows);
                num_rows++;
                Toast.makeText(getApplicationContext(), "Latitude: " + l.getLatitude() + "\nLongitude: " + l.getLongitude(), Toast.LENGTH_LONG).show(); // вывод в тосте
            }
        });


        btnDist.setOnClickListener(v -> new BigQueryTaskForRequest().execute(JsonRows));

        btnDelData.setOnClickListener(v -> new BigQueryTaskForDelete().execute(JsonRows));

    }

    private class BigQueryTask extends AsyncTask<String, Integer, String> {
        @Override
        protected void onPreExecute() {
            super.onPreExecute();
        }

        @Override
        protected String doInBackground(String... params) {
            try {

                InputStream is =
                        getApplicationContext().getAssets().open("CleveronAKMarkKalda-1dee716ac6ea.json");
                BigQuery bigquery = BigQueryOptions.newBuilder()
                        .setProjectId("cleveronak")
                        .setCredentials(ServiceAccountCredentials.fromStream(is))
                        .build().getService();

                TableId tableId = TableId.of(datasetName, "geo_points");

                int num = 0;
                Log.d("Main", "Sending JSON: " + csv_content);
                WriteChannelConfiguration configuration = WriteChannelConfiguration.newBuilder(tableId, FormatOptions.csv()).build();

                try (WriteChannel channel = bigquery.writer(configuration)) {
                    num = channel.write(ByteBuffer.wrap(csv_content.getBytes(StandardCharsets.UTF_8)));
                } catch (IOException e) {
                    Log.d("Main", e.toString());
                }
                Log.d("Main", "Loading " + Integer.toString(num) + " bytes into table " + tableId);

            } catch (IOException e) {
                e.printStackTrace();
            }
            return "Done";
        }

        @Override
        protected void onProgressUpdate(Integer... values) {
            super.onProgressUpdate(values);
        }

        @Override
        protected void onPostExecute(String msg) {
            super.onPostExecute(msg);
            Log.d("Main", "onPostExecute: " + msg);
        }
    }

    private class BigQueryTaskForRequest extends AsyncTask<String, Integer, String> {
        @Override
        protected void onPreExecute() {
            super.onPreExecute();
        }

        @Override
        protected String doInBackground(String... params) {
            try {

                InputStream is =
                        getApplicationContext().getAssets().open("CleveronAKMarkKalda-1dee716ac6ea.json");
                BigQuery bigquery = BigQueryOptions.newBuilder()
                        .setProjectId("cleveronak")
                        .setCredentials(ServiceAccountCredentials.fromStream(is))
                        .build().getService();

                QueryJobConfiguration queryConfig =
                        QueryJobConfiguration.newBuilder(distanceQuery)
                                .setUseLegacySql(false)
                                .build();

                JobId jobId = JobId.of(UUID.randomUUID().toString());
                Job queryJob = bigquery
                        .create(JobInfo
                                .newBuilder(queryConfig)
                                .setJobId(jobId).build());
                queryJob = queryJob.waitFor();


                if (queryJob == null) {
                    throw new RuntimeException("Job no longer exists");
                } else if (queryJob.getStatus().getError() != null) {
                    throw new
                            RuntimeException(queryJob.getStatus().getError().toString());
                }

                TableResult result = queryJob.getQueryResults();
                for (FieldValueList row : result.getValues()) {
                    distance = row.get(0);
                }
                distanceText.setText("Distance is: " + distance.getDoubleValue() + " meters");
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            return "Done";
        }

        @Override
        protected void onProgressUpdate(Integer... values) {
            super.onProgressUpdate(values);
        }

        @Override
        protected void onPostExecute(String msg) {
            super.onPostExecute(msg);
            Log.d("Main", "onPostExecute: " + msg);
        }
    }

    private class BigQueryTaskForDelete extends AsyncTask<String, Integer, String> {
        @Override
        protected void onPreExecute() {
            super.onPreExecute();
        }

        @Override
        protected String doInBackground(String... params) {
            try {

                InputStream is =
                        getApplicationContext().getAssets().open("CleveronAKMarkKalda-1dee716ac6ea.json");
                BigQuery bigquery = BigQueryOptions.newBuilder()
                        .setProjectId("cleveronak")
                        .setCredentials(ServiceAccountCredentials.fromStream(is))
                        .build().getService();

                QueryJobConfiguration queryConfig =
                        QueryJobConfiguration.newBuilder(deleteQuery)
                                .setUseLegacySql(false)
                                .build();

                JobId jobId = JobId.of(UUID.randomUUID().toString());
                Job queryJob = bigquery
                        .create(JobInfo
                                .newBuilder(queryConfig)
                                .setJobId(jobId).build());
                queryJob = queryJob.waitFor();

                if (queryJob == null) {
                    throw new RuntimeException("Job no longer exists");
                } else if (queryJob.getStatus().getError() != null) {
                    throw new
                            RuntimeException(queryJob.getStatus().getError().toString());
                }

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            return "Done";
        }

        @Override
        protected void onProgressUpdate(Integer... values) {
            super.onProgressUpdate(values);
        }

        @Override
        protected void onPostExecute(String msg) {
            super.onPostExecute(msg);
            Log.d("Main", "onPostExecute: " + msg);

        }
    }


}