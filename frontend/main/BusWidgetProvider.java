package com.example.mywidget;

import android.appwidget.AppWidgetManager;
import android.appwidget.AppWidgetProvider;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.util.Log;
import android.widget.RemoteViews;
import android.app.PendingIntent;
import android.os.Handler;
import android.os.Looper;
import android.os.Bundle;

import java.util.List;

public class BusWidgetProvider extends AppWidgetProvider {

    private static final String PREFS_NAME = "BusWidgetPrefs";
    private static final String PREF_USER_UUID = "user_uuid";
    private static final String PREF_WIDGET_MODE = "widget_mode_";
    private static final String PREF_STOPS_LOADED = "stops_loaded";

    public static final String ACTION_SELECT_FROM = "SELECT_FROM_STATION";
    public static final String ACTION_SELECT_TO = "SELECT_TO_STATION";
    public static final String ACTION_SELECT_COUNT = "SELECT_BUS_COUNT";
    public static final String ACTION_REFRESH = "REFRESH_WIDGET";
    public static final String ACTION_INITIAL_SETUP = "INITIAL_SETUP_DONE";
    public static final String ACTION_ITEM_CLICK = "ITEM_CLICK_ACTION";
    public static final String ACTION_FINISH_SETUP = "FINISH_SETUP";
    public static final String ACTION_BACK_TO_SETUP = "BACK_TO_SETUP";
    public static final String ACTION_REFRESH_DASHBOARD = "REFRESH_DASHBOARD";

    private static boolean isLoadingStops = false;

    @Override
    public void onUpdate(Context context, AppWidgetManager appWidgetManager, int[] appWidgetIds) {
        loadStopsOnce(context);

        for (int appWidgetId : appWidgetIds) {
            updateAppWidget(context, appWidgetManager, appWidgetId);
        }
    }

    private void loadStopsOnce(Context context) {
        SharedPreferences prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        boolean stopsLoaded = prefs.getBoolean(PREF_STOPS_LOADED, false);

        if (!stopsLoaded && !isLoadingStops) {
            isLoadingStops = true;
            new ApiClient(context).getStops(new ApiClient.StopsCallback() {
                @Override
                public void onSuccess(List<String> stops) {
                    isLoadingStops = false;
                    prefs.edit().putBoolean(PREF_STOPS_LOADED, true).apply();
                    Log.d("BusWidget", "Stops loaded successfully once");
                }

                @Override
                public void onError(String error) {
                    isLoadingStops = false;
                    Log.e("BusWidget", "Failed to load stops: " + error);
                }
            });
        }
    }

    @Override
    public void onReceive(Context context, Intent intent) {
        String action = intent.getAction();
        if (action == null) {
            super.onReceive(context, intent);
            return;
        }

        int appWidgetId = intent.getIntExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, AppWidgetManager.INVALID_APPWIDGET_ID);

        switch (action) {
            case ACTION_SELECT_FROM:
                openSelectionActivity(context, appWidgetId, "from");
                break;
            case ACTION_SELECT_TO:
                openSelectionActivity(context, appWidgetId, "to");
                break;
            case ACTION_SELECT_COUNT:
                openSelectionActivity(context, appWidgetId, "count");
                break;
            case ACTION_REFRESH:
            case ACTION_BACK_TO_SETUP:
            case ACTION_INITIAL_SETUP:
                setWidgetMode(context, appWidgetId, "setup");
                refreshWidget(context, appWidgetId);
                break;
            case ACTION_FINISH_SETUP:
                setWidgetMode(context, appWidgetId, "dashboard");
                refreshWidget(context, appWidgetId);

                AppWidgetManager manager = AppWidgetManager.getInstance(context);
                manager.notifyAppWidgetViewDataChanged(appWidgetId, R.id.dashboardBusesList);

                new Handler(Looper.getMainLooper()).postDelayed(() -> {
                    manager.notifyAppWidgetViewDataChanged(appWidgetId, R.id.dashboardBusesList);
                }, 300);
                break;
            case ACTION_REFRESH_DASHBOARD:
                SharedPreferences prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
                prefs.edit().putBoolean("manual_refresh_" + appWidgetId, true).apply();
                AppWidgetManager manage = AppWidgetManager.getInstance(context);
                manage.notifyAppWidgetViewDataChanged(appWidgetId, R.id.dashboardBusesList);
                break;
            case ACTION_ITEM_CLICK:
                int clickedPos = intent.getIntExtra("clicked_position", -1);
                String clickedTrackId = intent.getStringExtra("clicked_track_id");
                if (clickedPos != -1 && clickedTrackId != null) {
                    handleItemClick(context, appWidgetId, clickedTrackId);
                }
                break;
            default:
                super.onReceive(context, intent);
                break;
        }
    }

    private void setWidgetMode(Context context, int appWidgetId, String mode) {
        SharedPreferences prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        prefs.edit().putString(PREF_WIDGET_MODE + appWidgetId, mode).apply();
    }

    private void handleItemClick(Context context, int appWidgetId, String trackId) {
        String userId = getOrCreateUserUUID(context);
        SharedPreferences prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        String subscribedKey = "subscribed_" + trackId;
        boolean isSubscribed = prefs.getBoolean(subscribedKey, false);

        ApiClient apiClient = new ApiClient(context);
        if (!isSubscribed) {
            apiClient.subscribe(trackId, userId, new ApiClient.SubscribeCallback() {
                @Override
                public void onSuccess() {
                    prefs.edit().putBoolean(subscribedKey, true).apply();
                    refreshWidgetAfterSubscription(context, appWidgetId);
                }

                @Override
                public void onError(String error) {
                    Log.e("BusWidget", "Subscribe error: " + error);
                }
            });
        } else {
            apiClient.unsubscribe(trackId, userId, new ApiClient.SubscribeCallback() {
                @Override
                public void onSuccess() {
                    prefs.edit().remove(subscribedKey).apply();
                    refreshWidgetAfterSubscription(context, appWidgetId);
                }

                @Override
                public void onError(String error) {
                    Log.e("BusWidget", "Unsubscribe error: " + error);
                }
            });
        }
    }

    private void refreshWidgetAfterSubscription(Context context, int appWidgetId) {
        AppWidgetManager manager = AppWidgetManager.getInstance(context);
        manager.notifyAppWidgetViewDataChanged(appWidgetId, R.id.busesList);
        manager.notifyAppWidgetViewDataChanged(appWidgetId, R.id.dashboardBusesList);
    }

    private static String getOrCreateUserUUID(Context context) {
        SharedPreferences prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        String uuid = prefs.getString(PREF_USER_UUID, null);
        if (uuid == null || uuid.isEmpty()) {
            uuid = java.util.UUID.randomUUID().toString();
            prefs.edit().putString(PREF_USER_UUID, uuid).apply();
        }
        return uuid;
    }

    static void updateAppWidget(Context context, AppWidgetManager appWidgetManager, int appWidgetId) {
        SharedPreferences prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        String mode = prefs.getString(PREF_WIDGET_MODE + appWidgetId, "setup");

        if ("dashboard".equals(mode)) {
            updateDashboardMode(context, appWidgetManager, appWidgetId);
        } else {
            updateSetupMode(context, appWidgetManager, appWidgetId, prefs);
        }
    }

    private static void updateDashboardMode(Context context, AppWidgetManager appWidgetManager, int appWidgetId) {
        RemoteViews views = new RemoteViews(context.getPackageName(), R.layout.dashboard_widget_layout);

        Intent backIntent = new Intent(context, BusWidgetProvider.class);
        backIntent.setAction(ACTION_BACK_TO_SETUP);
        backIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        PendingIntent backPending = PendingIntent.getBroadcast(context, appWidgetId + 5000,
                backIntent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        views.setOnClickPendingIntent(R.id.backToSetupButton, backPending);

        Intent refreshIntent = new Intent(context, BusWidgetProvider.class);
        refreshIntent.setAction(ACTION_REFRESH_DASHBOARD);
        refreshIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        PendingIntent refreshPending = PendingIntent.getBroadcast(context, appWidgetId + 6000,
                refreshIntent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        views.setOnClickPendingIntent(R.id.refreshButton, refreshPending);

        Intent serviceIntent = new Intent(context, DashboardWidgetService.class);
        serviceIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        views.setRemoteAdapter(R.id.dashboardBusesList, serviceIntent);

        appWidgetManager.updateAppWidget(appWidgetId, views);
    }

    private static void updateSetupMode(Context context, AppWidgetManager appWidgetManager, int appWidgetId, SharedPreferences prefs) {
        String stopsString = prefs.getString("all_stops", "");

        RemoteViews views;
        if (stopsString.isEmpty()) {
            views = new RemoteViews(context.getPackageName(), R.layout.initial_setup_layout);
            Intent setupIntent = new Intent(context, BusWidgetProvider.class);
            setupIntent.setAction(ACTION_INITIAL_SETUP);
            setupIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
            PendingIntent pendingIntent = PendingIntent.getBroadcast(
                    context, appWidgetId + 5000, setupIntent,
                    PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
            views.setOnClickPendingIntent(R.id.btnSetup, pendingIntent);
        } else {
            views = new RemoteViews(context.getPackageName(), R.layout.widget_layout);
            String fromStation = prefs.getString("from_station" + appWidgetId, "Выбрать");
            String toStation = prefs.getString("to_station" + appWidgetId, "Выбрать");
            int busCount = prefs.getInt("bus_count" + appWidgetId, 5);

            views.setTextViewText(R.id.fromStationText, fromStation);
            views.setTextViewText(R.id.toStationText, toStation);
            views.setTextViewText(R.id.busCountText, String.valueOf(busCount));

            setupNormalWidgetClickHandlers(context, views, appWidgetId);
        }
        appWidgetManager.updateAppWidget(appWidgetId, views);
        if (!stopsString.isEmpty()) {
            appWidgetManager.notifyAppWidgetViewDataChanged(appWidgetId, R.id.busesList);
        }
    }

    private static void setupNormalWidgetClickHandlers(Context context, RemoteViews views, int appWidgetId) {
        Intent fromIntent = new Intent(context, BusWidgetProvider.class);
        fromIntent.setAction(ACTION_SELECT_FROM);
        fromIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        PendingIntent fromPending = PendingIntent.getBroadcast(context, appWidgetId,
                fromIntent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        views.setOnClickPendingIntent(R.id.fromCard, fromPending);

        Intent toIntent = new Intent(context, BusWidgetProvider.class);
        toIntent.setAction(ACTION_SELECT_TO);
        toIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        PendingIntent toPending = PendingIntent.getBroadcast(context, appWidgetId + 1000,
                toIntent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        views.setOnClickPendingIntent(R.id.toCard, toPending);

        Intent countIntent = new Intent(context, BusWidgetProvider.class);
        countIntent.setAction(ACTION_SELECT_COUNT);
        countIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        PendingIntent countPending = PendingIntent.getBroadcast(context, appWidgetId + 2000,
                countIntent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        views.setOnClickPendingIntent(R.id.busCountCard, countPending);

        Intent confirmIntent = new Intent(context, BusWidgetProvider.class);
        confirmIntent.setAction(ACTION_REFRESH);
        confirmIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        PendingIntent confirmPending = PendingIntent.getBroadcast(context, appWidgetId + 3000,
                confirmIntent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        views.setOnClickPendingIntent(R.id.confirmButton, confirmPending);

        Intent finishIntent = new Intent(context, BusWidgetProvider.class);
        finishIntent.setAction(ACTION_FINISH_SETUP);
        finishIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        PendingIntent finishPending = PendingIntent.getBroadcast(context, appWidgetId + 4000,
                finishIntent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        views.setOnClickPendingIntent(R.id.finishSetupButton, finishPending);

        Intent serviceIntent = new Intent(context, BusWidgetService.class);
        serviceIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        views.setRemoteAdapter(R.id.busesList, serviceIntent);

        Intent templateIntent = new Intent(context, BusWidgetProvider.class);
        templateIntent.setAction(ACTION_ITEM_CLICK);
        templateIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        PendingIntent templatePendingIntent = PendingIntent.getBroadcast(
                context, appWidgetId, templateIntent,
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_MUTABLE);
        views.setPendingIntentTemplate(R.id.busesList, templatePendingIntent);
    }

    private void openSelectionActivity(Context context, int appWidgetId, String type) {
        Intent activityIntent = new Intent(context, SelectionActivity.class);
        activityIntent.putExtra("type", type);
        activityIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId);
        activityIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        context.startActivity(activityIntent);
    }

    private void refreshWidget(Context context, int appWidgetId) {
        SharedPreferences prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        prefs.edit().putBoolean("is_confirmed" + appWidgetId, true).apply();
        AppWidgetManager manager = AppWidgetManager.getInstance(context);
        updateAppWidget(context, manager, appWidgetId);
    }

    @Override
    public void onAppWidgetOptionsChanged(Context context, AppWidgetManager appWidgetManager,
                                          int appWidgetId, Bundle newOptions) {
        super.onAppWidgetOptionsChanged(context, appWidgetManager, appWidgetId, newOptions);

        Log.d("BusWidgetProvider", "Widget resized: " + appWidgetId);

        updateAppWidget(context, appWidgetManager, appWidgetId);
    }
}