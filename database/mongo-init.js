db = db.getSiblingDB('f1_analytics');

db.createUser({
    user: "f1_app_user",
    pwd: "f1banjutjaya", 
    roles: [
        { role: "readWrite", db: "f1_analytics" }
    ]
});

db.createCollection(
    "telemetry_live",
    {
       timeseries: {
          timeField: "timestamp",     
          metaField: "metadata",     
          granularity: "seconds"
       },
    }
);

db.createCollection(
    "lap_times",
    {
       timeseries: {
          timeField: "timestamp",
          metaField: "metadata",
          granularity: "minutes"
       }
    }
);
print("Time-Series Collection 'lap_times' berhasil dibuat.");

db.telemetry_live.createIndex(
    { "metadata.session_id": 1, "metadata.driver_code": 1, "timestamp": 1 },
    { name: "idx_session_driver_time" }
);

db.lap_times.createIndex(
    { "metadata.session_id": 1, "metadata.driver_code": 1 },
    { name: "idx_session_driver" }
);