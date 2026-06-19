package com.example.mywidget;

import android.appwidget.AppWidgetManager;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.widget.RemoteViews;
import android.widget.RemoteViewsService;
import android.graphics.Color;

import java.util.ArrayList;
import java.util.List;

public class BusWidgetService extends RemoteViewsService {
    @Override
    public RemoteViewsFactory onGetViewFactory(Intent intent) {
        int appWidgetId = intent.getIntExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, AppWidgetManager.INVALID_APPWIDGET_ID);
        return new BusRemoteViewsFactory(getApplicationContext(), appWidgetId);
    }

    class BusRemoteViewsFactory implements RemoteViewsService.RemoteViewsFactory {
        private Context context;
        private int appWidgetId;
        private List<Bus> buses;
        private SharedPreferences prefs;
        private ApiClient apiClient;

        public BusRemoteViewsFactory(Context context, int appWidgetId) {
            this.context = context;
            this.appWidgetId = appWidgetId;
            this.prefs = context.getSharedPreferences("BusWidgetPrefs", Context.MODE_PRIVATE);
            this.apiClient = new ApiClient(context);
            this.buses = new ArrayList<>();
        }

        @Override
        public void onCreate() {}

        @Override
        public void onDataSetChanged() {
            loadBusesFromApi();
        }

        private void loadBusesFromApi() {
            String fromStation = prefs.getString("from_station" + appWidgetId, "");
            String toStation = prefs.getString("to_station" + appWidgetId, "");
            int busCount = prefs.getInt("bus_count" + appWidgetId, 5);
            boolean isConfirmed = prefs.getBoolean("is_confirmed" + appWidgetId, false);

            if (!isConfirmed || fromStation.isEmpty() || toStation.isEmpty() ||
                    "Выбрать".equals(fromStation) || "Выбрать".equals(toStation)) {
                buses.clear();
                return;
            }

            List<Bus> allBuses = fetchBusesSync(fromStation, toStation);
            if (allBuses == null || allBuses.isEmpty()) {
                buses.clear();
                return;
            }

            for (Bus bus : allBuses) {
                boolean sub = prefs.getBoolean("subscribed_" + bus.trackId, false);
                bus.setSubscribed(sub);
            }

            allBuses.sort((b1, b2) -> {
                boolean b1Valid = "active".equals(b1.status) && b1.hasValidTime();
                boolean b2Valid = "active".equals(b2.status) && b2.hasValidTime();
                boolean b1Pending = "pending".equals(b1.status);
                boolean b2Pending = "pending".equals(b2.status);
                if (b1Valid && b2Valid) return Integer.compare(b1.getTimeInMinutes(), b2.getTimeInMinutes());
                if (b1Valid) return -1;
                if (b2Valid) return 1;
                if (b1Pending && !b2Pending) return -1;
                if (!b1Pending && b2Pending) return 1;
                return 0;
            });

            buses = (allBuses.size() > busCount) ? allBuses.subList(0, busCount) : allBuses;
        }

        private List<Bus> fetchBusesSync(String fromStation, String toStation) {
            final List<Bus>[] result = new List[1];
            final boolean[] completed = {false};
            final Object lock = new Object();

            apiClient.getBuses(fromStation, toStation, new ApiClient.BusesCallback() {
                @Override
                public void onSuccess(List<Bus> loadedBuses) {
                    result[0] = loadedBuses;
                    synchronized (lock) { completed[0] = true; lock.notify(); }
                }
                @Override
                public void onError(String error) {
                    synchronized (lock) { completed[0] = true; lock.notify(); }
                }
            });

            synchronized (lock) {
                if (!completed[0]) {
                    try { lock.wait(5000); } catch (InterruptedException e) {}
                }
            }
            return result[0];
        }

        @Override
        public RemoteViews getViewAt(int position) {
            if (position >= buses.size()) return null;
            Bus bus = buses.get(position);
            RemoteViews views = new RemoteViews(context.getPackageName(), R.layout.bus_item_widget);

            views.setTextViewText(R.id.busNumber, bus.number);
            views.setTextViewText(R.id.fromStationName, bus.fromStation);
            views.setTextViewText(R.id.fromStationTime, bus.fromTime);
            views.setTextViewText(R.id.toStationName, bus.toStation);
            views.setTextViewText(R.id.toStationTime, bus.toTime);

            views.setTextViewText(R.id.totalTime, bus.totalTime);
            views.setTextColor(R.id.totalTime, Color.parseColor("#4CAF50"));

            if (bus.isSubscribed()) {
                views.setInt(R.id.bus_item_root, "setBackgroundResource", R.drawable.widget_card_bg_selected);
                views.setTextViewText(R.id.selectButton, "★");
                views.setTextColor(R.id.selectButton, Color.parseColor("#FFD700"));
            } else {
                views.setInt(R.id.bus_item_root, "setBackgroundResource", R.drawable.widget_card_bg_without_rad);
                views.setTextViewText(R.id.selectButton, "✓");
                views.setTextColor(R.id.selectButton, Color.parseColor("#FFFFFF"));
            }

            Intent fillInIntent = new Intent();
            fillInIntent.putExtra("clicked_position", position);
            fillInIntent.putExtra("clicked_track_id", bus.trackId);
            views.setOnClickFillInIntent(R.id.selectButton, fillInIntent);

            return views;
        }

        @Override public RemoteViews getLoadingView() { return null; }
        @Override public int getCount() { return buses.size(); }
        @Override public int getViewTypeCount() { return 1; }
        @Override public long getItemId(int position) { return position; }
        @Override public boolean hasStableIds() { return true; }
        @Override public void onDestroy() { buses.clear(); }
    }
}