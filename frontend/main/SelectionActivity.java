package com.example.mywidget;

import android.appwidget.AppWidgetManager;
import android.content.Context;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.View;
import android.widget.*;
import androidx.appcompat.app.AppCompatActivity;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SelectionActivity extends AppCompatActivity {

    private String type;
    private int appWidgetId;
    private SharedPreferences prefs;
    private ApiClient apiClient;
    private List<String> allStations = new ArrayList<>();
    private final List<String> filteredStations = new ArrayList<>();
    private EditText searchEditText;
    private ListView listView;
    private ProgressBar progressBar;
    private ArrayAdapter<String> adapter;
    private boolean isLoading = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        type = getIntent().getStringExtra("type");
        appWidgetId = getIntent().getIntExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, AppWidgetManager.INVALID_APPWIDGET_ID);
        prefs = getSharedPreferences("BusWidgetPrefs", Context.MODE_PRIVATE);
        apiClient = new ApiClient(this);

        if (type != null && type.equals("dashboard_return")) {
            finish();
            return;
        }

        if (type != null && type.equals("count")) {
            setContentView(R.layout.activity_count_selection);
            setupCountSelector();
        } else {
            setContentView(R.layout.activity_selection);
            setupStationSelector();
        }
    }

    private void setupStationSelector() {
        TextView title = findViewById(R.id.selectionTitle);
        searchEditText = findViewById(R.id.searchEditText);
        listView = findViewById(R.id.selectionList);

        String titleText = (type != null && type.equals("from"))
                ? "Выберите остановку отправления"
                : "Выберите остановку прибытия";
        title.setText(titleText);

        searchEditText.setVisibility(View.VISIBLE);

        adapter = new ArrayAdapter<>(this, android.R.layout.simple_list_item_1, filteredStations);
        listView.setAdapter(adapter);

        searchEditText.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {}

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {
                filterStations(s.toString());
            }

            @Override
            public void afterTextChanged(Editable s) {}
        });

        progressBar = new ProgressBar(this);
        progressBar.setLayoutParams(new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.WRAP_CONTENT,
                LinearLayout.LayoutParams.WRAP_CONTENT));

        View rootView = findViewById(android.R.id.content);
        if (rootView instanceof LinearLayout) {
            ((LinearLayout) rootView).addView(progressBar);
        }

        allStations = apiClient.getStopsFromPrefs();
        filteredStations.addAll(allStations);
        adapter.notifyDataSetChanged();
        progressBar.setVisibility(View.GONE);

        listView.setOnItemClickListener((parent, view, position, id) -> {
            String selected = filteredStations.get(position);
            saveSelection(selected);
            finish();
        });
    }

    private void filterStations(String query) {
        filteredStations.clear();

        if (query == null || query.trim().isEmpty()) {
            filteredStations.addAll(allStations);
        } else {
            String lowerQuery = query.toLowerCase(Locale.getDefault());
            for (String station : allStations) {
                if (station.toLowerCase(Locale.getDefault()).startsWith(lowerQuery)) {
                    filteredStations.add(station);
                }
            }
        }

        adapter.notifyDataSetChanged();
    }

    private void setupCountSelector() {
        SeekBar seekBar = findViewById(R.id.countSeekBar);
        TextView valueText = findViewById(R.id.countValue);
        Button confirmButton = findViewById(R.id.confirmCountButton);

        int current = prefs.getInt("bus_count" + appWidgetId, 5);
        if (current < 1) current = 5;
        seekBar.setProgress(current);
        valueText.setText(String.valueOf(current));

        seekBar.setOnSeekBarChangeListener(new SeekBar.OnSeekBarChangeListener() {
            @Override
            public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
                if (progress < 1) progress = 1;
                valueText.setText(String.valueOf(progress));
            }
            @Override
            public void onStartTrackingTouch(SeekBar seekBar) {}
            @Override
            public void onStopTrackingTouch(SeekBar seekBar) {}
        });

        confirmButton.setOnClickListener(v -> {
            int count = seekBar.getProgress();
            if (count < 1) count = 1;
            saveCount(count);
            finish();
        });
    }

    private void saveSelection(String value) {
        String key = (type != null && type.equals("from"))
                ? "from_station" + appWidgetId
                : "to_station" + appWidgetId;
        prefs.edit().putString(key, value).apply();

        prefs.edit().putBoolean("is_confirmed" + appWidgetId, false).apply();

        updateWidget();
    }

    private void saveCount(int count) {
        prefs.edit().putInt("bus_count" + appWidgetId, count).apply();

        // Сбрасываем флаг подтверждения при смене количества
        prefs.edit().putBoolean("is_confirmed" + appWidgetId, false).apply();

        updateWidget();
    }

    private void updateWidget() {
        AppWidgetManager manager = AppWidgetManager.getInstance(this);
        BusWidgetProvider.updateAppWidget(this, manager, appWidgetId);
    }
}