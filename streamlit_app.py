import streamlit as st
import pandas as pd
import json
import os
import time
from datetime import datetime

# --- File Path Configuration ---
METRICS_FILE = 'stream_metrics.json' 
ALERTS_FILE = 'stream_alerts.log'

# --- Functions to Load Data ---

def load_stream_metrics():
    """Loads stream processing metrics from stream_metrics.json."""
    if not os.path.exists(METRICS_FILE):
        return {"error": f"File '{METRICS_FILE}' not found. Please run the Kafka consumer (project.py)."}
    try:
        # Re-read for 'live' updates
        with open(METRICS_FILE, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {"error": f"JSON decoding error in '{METRICS_FILE}'. The file might be currently being written to."}
    except Exception as e:
        return {"error": f"File reading error: {e}"}

def load_alert_logs():
    """Loads alert logs from stream_alerts.log."""
    if not os.path.exists(ALERTS_FILE):
        return [f"File '{ALERTS_FILE}' not found. (Consumer: project.py must be running)"]
    try:
        with open(ALERTS_FILE, 'r') as f:
            # Read all lines
            return [line.strip() for line in f.readlines()]
    except Exception as e:
        return [f"Error reading alert file: {e}"]

# --- Streamlit App Layout ---

st.set_page_config(layout="wide")
st.title("IMDB/Wikimedia Stream Processing Dashboard")
st.markdown("Metrics updated every 2 seconds by reading `stream_metrics.json` and `stream_alerts.log`.")

# Main loop to refresh the data
placeholder = st.empty()
while True:
    metrics = load_stream_metrics()
    alert_logs = load_alert_logs()

    with placeholder.container():
        
        # --- DISPLAY ERROR/LOADER ---
        if "error" in metrics:
            st.error(metrics["error"])
            time.sleep(2)
            continue
        
        # --- LAYOUT FOR METRICS ---
        st.header("1. Real-Time Metrics from Wikimedia Stream")
        col1, col2, col3, col4 = st.columns(4)
        
        # Total Events
        with col1:
            st.metric("Total Events Processed", metrics.get("total_events", 0))
            
        # Total Alerts
        with col2:
            st.metric("Total Alerts Triggered", metrics.get("total_alerts", 0))
        
        # Change Types (Example: Edit, Categorize)
        with col3:
            st.markdown("##### Event Types (Top 2)")
            change_types = metrics.get("change_types", {})
            # Sort and display the top 2 change types
            sorted_types = sorted(change_types.items(), key=lambda item: item[1], reverse=True)
            
            if sorted_types:
                st.code(f"{sorted_types[0][0]}: {sorted_types[0][1]}", language='text')
                if len(sorted_types) > 1:
                     st.code(f"{sorted_types[1][0]}: {sorted_types[1][1]}", language='text')
            else:
                st.info("No event types recorded.")
        
        # Update Time
        with col4:
            st.metric("Last Data Update", datetime.now().strftime("%H:%M:%S"))

        # --- DISPLAY ENTITY BREAKDOWN ---
        col_m1, col_m2 = st.columns([1.5, 1])

        with col_m1:
            st.subheader("2. Detailed Event Counts")
            st.markdown("##### Events per Tracked Entity")
            
            # utiliser 'entity_counts' au lieu de 'events_per_entity'
            entity_data = metrics.get("entity_counts", {}) 
            
            if entity_data:
                # Convert dict to DataFrame for better visualization
                df_entities = pd.DataFrame(list(entity_data.items()), columns=['Entity', 'Count'])
                
                # Plot the data using a bar chart
                st.bar_chart(df_entities.set_index('Entity'))
                
                # Display the data frame as well
                st.dataframe(df_entities.sort_values(by='Count', ascending=False).reset_index(drop=True), 
                             use_container_width=True, hide_index=True)
            else:
                st.warning("No entity data found. Is the pipeline running?")

        # --- DISPLAY ALERTS ---
        with col_m2:
            st.subheader("🚨 Alerting System (User: 'Joe Bloggs')")
            
            if len(alert_logs) == 0 or alert_logs == [f"File '{ALERTS_FILE}' not found. (Consumer: project.py must be running)"]:
                 st.warning("No alerts found. Ensure the consumer is running and an alert event has been simulated.")
            else:
                st.success(f"**{len(alert_logs)}** alert(s) recorded.")
                
                # Display alerts
                for i, log in enumerate(alert_logs):
                    st.code(log, language='log')
                st.caption(f"Alerts are routed to the file `{ALERTS_FILE}`, as required by the project specifications.")
    
    # Wait 2 seconds before the next update
    time.sleep(2)