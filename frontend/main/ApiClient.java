package com.example.mywidget;

import android.content.Context;
import android.content.SharedPreferences;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApiClient {
    private static final String BASE_URL = "http://37.194.2.116:8000";
    private static final String PREFS_NAME = "BusWidgetPrefs";
    private Context context;

    public ApiClient(Context context) {
        this.context = context;
    }

    public interface StopsCallback {
        void onSuccess(List<String> stops);
        void onError(String error);
    }

    public interface BusesCallback {
        void onSuccess(List<Bus> buses);
        void onError(String error);
    }

    public interface SubscribeCallback {
        void onSuccess();
        void onError(String error);
    }

    public interface DashboardCallback {
        void onSuccess(List<DashboardRoute> routes);
        void onError(String error);
    }

    public void getStops(StopsCallback callback) {
        new Thread(() -> {
            try {
                URL url = new URL(BASE_URL + "/stops");
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(10000);
                conn.setReadTimeout(10000);

                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) response.append(line);
                    reader.close();

                    JSONObject json = new JSONObject(response.toString());
                    JSONArray stopsArray = json.getJSONArray("stops");
                    List<String> stops = new ArrayList<>();
                    for (int i = 0; i < stopsArray.length(); i++)
                        stops.add(stopsArray.getString(i));

                    saveStopsToPrefs(stops);
                    callback.onSuccess(stops);
                } else {
                    callback.onError("HTTP " + responseCode);
                }
                conn.disconnect();
            } catch (Exception e) {
                callback.onError(e.getMessage());
            }
        }).start();
    }

    public void getBuses(String fromStation, String toStation, BusesCallback callback) {
        new Thread(() -> {
            try {
                URL url = new URL(BASE_URL + "/get_buses?start=" + URLEncoder.encode(fromStation, "UTF-8") +
                        "&end=" + URLEncoder.encode(toStation, "UTF-8"));
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(10000);
                conn.setReadTimeout(10000);

                if (conn.getResponseCode() == 200) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) response.append(line);
                    reader.close();

                    List<Bus> buses = parseBusesFromJson(response.toString(), fromStation, toStation);
                    callback.onSuccess(buses);
                } else {
                    callback.onError("HTTP " + conn.getResponseCode());
                }
                conn.disconnect();
            } catch (Exception e) {
                callback.onError(e.getMessage());
            }
        }).start();
    }

    public void getDashboard(String userId, DashboardCallback callback) {
        new Thread(() -> {
            try {
                URL url = new URL(BASE_URL + "/dashboard?user_id=" + URLEncoder.encode(userId, "UTF-8"));
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(10000);
                conn.setReadTimeout(10000);

                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) response.append(line);
                    reader.close();

                    List<DashboardRoute> routes = parseDashboardFromJson(response.toString());
                    callback.onSuccess(routes);
                } else {
                    callback.onError("HTTP " + responseCode);
                }
                conn.disconnect();
            } catch (Exception e) {
                callback.onError(e.getMessage());
            }
        }).start();
    }

    public void refreshUserBuses(String userId, DashboardCallback callback) {
        new Thread(() -> {
            try {
                URL url = new URL(BASE_URL + "/refresh_user_buses?user_id=" + URLEncoder.encode(userId, "UTF-8"));
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setConnectTimeout(60000);
                conn.setReadTimeout(60000);

                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    getDashboard(userId, callback);
                } else {
                    callback.onError("HTTP " + responseCode);
                }
                conn.disconnect();
            } catch (Exception e) {
                callback.onError(e.getMessage());
            }
        }).start();
    }

    private List<DashboardRoute> parseDashboardFromJson(String jsonString) throws Exception {
        List<DashboardRoute> routes = new ArrayList<>();
        JSONArray jsonArray = new JSONArray(jsonString);

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            DashboardRoute route = new DashboardRoute();
            route.trackId = obj.optString("track_id", "");
            route.busNumber = obj.optString("bus", "Автобус");
            route.startStop = obj.optString("start_stop", "?");
            route.endStop = obj.optString("end_stop", "?");
            route.arrivalStart = obj.optString("arrival_start", "--:--");
            route.arrivalEnd = obj.optString("arrival_end", "--:--");
            route.status = obj.optString("status", "active");
            route.est_time = obj.optString("est_travel_time");
            routes.add(route);
        }

        return routes;
    }

    private List<Bus> parseBusesFromJson(String jsonString, String fromStation, String toStation) throws Exception {
        Map<String, Bus> uniqueBuses = new HashMap<>();
        JSONArray jsonArray = new JSONArray(jsonString);
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            String busNumber = obj.getString("bus");
            String status = obj.optString("status", "active");
            String arrivalStart = obj.getString("arrival_start");
            String arrivalEnd = obj.getString("arrival_end");
            String trackId = obj.optString("track_id");
            String est_travel_time = obj.optString("est_travel_time");

            String totalTime;
            if ("pending".equals(status)) {
                totalTime = est_travel_time + " мин";
            } else {
                totalTime = calculateTimeDifference(arrivalStart, arrivalEnd, est_travel_time);
                if (totalTime == null || totalTime.isEmpty()) totalTime = est_travel_time + " мин";
            }

            String uniqueKey = trackId + "_" + busNumber;
            if (!uniqueBuses.containsKey(uniqueKey)) {
                Bus bus = new Bus(busNumber, fromStation, arrivalStart, toStation, arrivalEnd, totalTime);
                bus.status = status;
                bus.trackId = trackId;
                uniqueBuses.put(uniqueKey, bus);
            }
        }
        List<Bus> buses = new ArrayList<>(uniqueBuses.values());
        buses.sort((b1, b2) -> {
            boolean b1Valid = "active".equals(b1.status) && b1.hasValidTime();
            boolean b2Valid = "active".equals(b2.status) && b2.hasValidTime();
            boolean b1Pending = "pending".equals(b1.status);
            boolean b2Pending = "pending".equals(b2.status);
            if (b1Valid && b2Valid)
                return Integer.compare(b1.getTimeInMinutes(), b2.getTimeInMinutes());
            if (b1Valid) return -1;
            if (b2Valid) return 1;
            if (b1Pending && !b2Pending) return -1;
            if (!b1Pending && b2Pending) return 1;
            return 0;
        });
        return buses;
    }

    private String calculateTimeDifference(String start, String end, String est_tr) {
        try {
            String[] startParts = start.split(":");
            String[] endParts = end.split(":");
            int startMinutes = Integer.parseInt(startParts[0]) * 60 + Integer.parseInt(startParts[1]);
            int endMinutes = Integer.parseInt(endParts[0]) * 60 + Integer.parseInt(endParts[1]);
            int diff = endMinutes - startMinutes;
            if (diff < 0) diff += 24 * 60;
            return diff + " мин";
        } catch (Exception e) {
            return est_tr + " мин";
        }
    }

    private void saveStopsToPrefs(List<String> stops) {
        SharedPreferences prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        StringBuilder sb = new StringBuilder();
        for (String s : stops) sb.append(s).append(";");
        prefs.edit().putString("all_stops", sb.toString()).apply();
    }

    public List<String> getStopsFromPrefs() {
        SharedPreferences prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        String stopsString = prefs.getString("all_stops", "");
        List<String> stops = new ArrayList<>();
        if (!stopsString.isEmpty()) {
            for (String part : stopsString.split(";")) if (!part.isEmpty()) stops.add(part);
        }
        return stops;
    }

    public void subscribe(String trackId, String userId, SubscribeCallback callback) {
        sendSubscriptionRequest("/subscribe", trackId, userId, callback);
    }

    public void unsubscribe(String trackId, String userId, SubscribeCallback callback) {
        sendSubscriptionRequest("/unsubscribe", trackId, userId, callback);
    }

    private void sendSubscriptionRequest(String endpoint, String trackId, String userId, SubscribeCallback callback) {
        new Thread(() -> {
            try {
                URL url = new URL(BASE_URL + endpoint + "?track_id=" + URLEncoder.encode(trackId, "UTF-8") +
                        "&user_id=" + URLEncoder.encode(userId, "UTF-8"));
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();

                if ("/subscribe".equals(endpoint)) {
                    conn.setRequestMethod("POST");
                } else if ("/unsubscribe".equals(endpoint)) {
                    conn.setRequestMethod("DELETE");
                } else {
                    callback.onError("Unknown endpoint");
                    return;
                }

                conn.setConnectTimeout(10000);
                conn.setReadTimeout(10000);
                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    callback.onSuccess();
                } else {
                    callback.onError("HTTP " + responseCode);
                }
                conn.disconnect();
            } catch (Exception e) {
                callback.onError(e.getMessage());
            }
        }).start();
    }

    public static class DashboardRoute {
        public String trackId;
        public String busNumber;
        public String startStop;
        public String endStop;
        public String arrivalStart;
        public String arrivalEnd;
        public String status;

        public String est_time;
    }
}