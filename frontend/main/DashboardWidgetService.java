package com.example.mywidget;

import android.appwidget.AppWidgetManager;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Handler;
import android.os.Looper;
import android.widget.RemoteViews;
import android.widget.RemoteViewsService;
import android.graphics.Color;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class DashboardWidgetService extends RemoteViewsService {

    @Override
    public RemoteViewsFactory onGetViewFactory(Intent intent) {
        int appWidgetId = intent.getIntExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, AppWidgetManager.INVALID_APPWIDGET_ID);
        return new DashboardRemoteViewsFactory(getApplicationContext(), appWidgetId);
    }

    class DashboardRemoteViewsFactory implements RemoteViewsService.RemoteViewsFactory {
        private Context context;
        private int appWidgetId;
        private List<ApiClient.DashboardRoute> routes;
        private SharedPreferences prefs;
        private String userId;
        private ApiClient apiClient;
        private Handler handler;
        private Runnable autoRefreshRunnable;
        private AtomicBoolean isLoading;
        private long lastLoadTime;

        private void updateLastUpdateTime() {
            String timeStr = new java.text.SimpleDateFormat("HH:mm", java.util.Locale.getDefault())
                    .format(new java.util.Date());
            RemoteViews updateViews = new RemoteViews(context.getPackageName(), R.layout.dashboard_widget_layout);
            updateViews.setTextViewText(R.id.lastUpdateText, timeStr);
            AppWidgetManager.getInstance(context).partiallyUpdateAppWidget(appWidgetId, updateViews);
        }

        public DashboardRemoteViewsFactory(Context context, int appWidgetId) {
            this.context = context;
            this.appWidgetId = appWidgetId;
            this.prefs = context.getSharedPreferences("BusWidgetPrefs", Context.MODE_PRIVATE);
            this.userId = getOrCreateUserUUID(context);
            this.apiClient = new ApiClient(context);
            this.routes = new ArrayList<>();
            this.handler = new Handler(Looper.getMainLooper());
            this.isLoading = new AtomicBoolean(false);
            this.lastLoadTime = 0;
        }

        @Override
        public void onCreate() {
            loadiDashboardFromApi();
            startAutoRefresh();
        }

        private void startAutoRefresh() {
            autoRefreshRunnable = new Runnable() {
                @Override
                public void run() {
                    loadiDashboardFromApi();
                    handler.postDelayed(this, 60000);
                }
            };
            handler.postDelayed(autoRefreshRunnable, 60000);
        }

        private void stopAutoRefresh() {
            if (autoRefreshRunnable != null) {
                handler.removeCallbacks(autoRefreshRunnable);
            }
        }

        @Override
        public void onDataSetChanged() {
            android.util.Log.d("DashboardWidget", "onDataSetChanged called");
            loadiDashboardFromApi();
        }

        private void loadiDashboardFromApi() {
            loadDashboardFromApi(false);
        }

        private void loadDashboardFromApi(boolean forceRefresh) {
            if (isLoading.get()) {
                android.util.Log.d("DashboardWidget", "Already loading, skipping...");
                return;
            }

            long now = System.currentTimeMillis();
            if (now - lastLoadTime < 5000) {
                android.util.Log.d("DashboardWidget", "Too frequent requests, skipping...");
                return;
            }

            isLoading.set(true);
            lastLoadTime = now;

            boolean manualRefresh = prefs.getBoolean("manual_refresh_" + appWidgetId, false);
            if (manualRefresh) {
                prefs.edit().remove("manual_refresh_" + appWidgetId).apply();
                forceRefresh = true;
            }

            android.util.Log.d("DashboardWidget", "Loading dashboard... forceRefresh = " + forceRefresh);

            if (forceRefresh) {
                apiClient.refreshUserBuses(userId, new ApiClient.DashboardCallback() {
                    @Override
                    public void onSuccess(List<ApiClient.DashboardRoute> dashboardRoutes) {
                        handleSuccess(dashboardRoutes);
                    }

                    @Override
                    public void onError(String error) {
                        android.util.Log.w("DashboardWidget", "Refresh failed, fallback to normal: " + error);
                        apiClient.getDashboard(userId, new ApiClient.DashboardCallback() {
                            @Override public void onSuccess(List<ApiClient.DashboardRoute> r) { handleSuccess(r); }
                            @Override public void onError(String e) { handleError(e); }
                        });
                    }
                });
            } else {
                apiClient.getDashboard(userId, new ApiClient.DashboardCallback() {
                    @Override public void onSuccess(List<ApiClient.DashboardRoute> r) { handleSuccess(r); }
                    @Override public void onError(String e) { handleError(e); }
                });
            }
        }

        private void handleSuccess(List<ApiClient.DashboardRoute> dashboardRoutes) {
            routes.clear();
            if (dashboardRoutes != null) {
                routes.addAll(dashboardRoutes);
            }
            isLoading.set(false);
            AppWidgetManager.getInstance(context).notifyAppWidgetViewDataChanged(appWidgetId, R.id.dashboardBusesList);
            updateLastUpdateTime();
        }

        private void handleError(String error) {
            android.util.Log.e("DashboardWidget", "Error loading dashboard: " + error);
            isLoading.set(false);
        }

        @Override
        public RemoteViews getViewAt(int position) {
            if (position >= routes.size()) return null;
            ApiClient.DashboardRoute route = routes.get(position);

            RemoteViews views = new RemoteViews(context.getPackageName(), R.layout.dashboard_route_card);

            views.setTextViewText(R.id.busNumber, route.busNumber);

            if ("pending".equals(route.status)) {
                views.setTextViewText(R.id.totalTime, route.est_time + " мин");
                views.setTextColor(R.id.totalTime, Color.parseColor("#4CAF50"));
            } else {
                String timeDiff = calculateTimeDifference(route.arrivalStart, route.arrivalEnd);
                views.setTextViewText(R.id.totalTime, timeDiff);
                views.setTextColor(R.id.totalTime, Color.parseColor("#4CAF50"));
            }

            views.setTextViewText(R.id.fromStationName, route.startStop);
            views.setTextViewText(R.id.fromStationTime, route.arrivalStart);
            views.setTextViewText(R.id.toStationName, route.endStop);
            views.setTextViewText(R.id.toStationTime, route.arrivalEnd);

            return views;
        }

        private String calculateTimeDifference(String start, String end) {
            try {
                String[] startParts = start.split(":");
                String[] endParts = end.split(":");
                int startMinutes = Integer.parseInt(startParts[0]) * 60 + Integer.parseInt(startParts[1]);
                int endMinutes = Integer.parseInt(endParts[0]) * 60 + Integer.parseInt(endParts[1]);
                int diff = endMinutes - startMinutes;
                if (diff < 0) diff += 24 * 60;
                return diff + " мин";
            } catch (Exception e) {
                return "Н/Д";
            }
        }

        @Override
        public void onDestroy() {
            stopAutoRefresh();
            routes.clear();
        }

        @Override public RemoteViews getLoadingView() { return null; }
        @Override public int getCount() { return routes.size(); }
        @Override public int getViewTypeCount() { return 1; }
        @Override public long getItemId(int position) { return position; }
        @Override public boolean hasStableIds() { return true; }
    }

    private static String getOrCreateUserUUID(Context context) {
        SharedPreferences prefs = context.getSharedPreferences("BusWidgetPrefs", Context.MODE_PRIVATE);
        String uuid = prefs.getString("user_uuid", null);
        if (uuid == null || uuid.isEmpty()) {
            uuid = java.util.UUID.randomUUID().toString();
            prefs.edit().putString("user_uuid", uuid).apply();
        }
        return uuid;
    }
}