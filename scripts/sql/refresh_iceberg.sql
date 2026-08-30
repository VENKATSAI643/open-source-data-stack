DROP TABLE IF EXISTS nyc_taxi_iceberg.yellow_trips;
CREATE TABLE nyc_taxi_iceberg.yellow_trips (
    vendor_id Nullable(Int32), 
    pickup_datetime Nullable(DateTime64(6, 'UTC')), 
    dropoff_datetime Nullable(DateTime64(6, 'UTC')), 
    passenger_count Nullable(Int32), 
    trip_distance Nullable(Float64), 
    pu_location_id Nullable(Int32), 
    do_location_id Nullable(Int32), 
    payment_type Nullable(Int32), 
    fare_amount Nullable(Float64), 
    tip_amount Nullable(Float64), 
    total_amount Nullable(Float64)
) ENGINE = Iceberg('http://minio:9000/warehouse/taxi/trips', 'minioadmin', 'minioadmin123');
