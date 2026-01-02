from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "raw" / "njt_rail"

routes = pd.read_csv(DATA_PATH / "routes.txt")
trips = pd.read_csv(DATA_PATH / "trips.txt")
stop_times = pd.read_csv(DATA_PATH / "stop_times.txt")
calendar = pd.read_csv(DATA_PATH / "calendar_dates.txt")

print("Routes:")
print(routes.head(), "\n")

print("Trips:")
print(trips.head(), "\n")

print("Stop Times:")
print(stop_times.head(), "\n")

print("Calendar:")
print(calendar.head())


# Select only useful columns for now
routes_simple = routes[["route_id", "route_long_name"]]
trips_simple = trips[["trip_id", "route_id"]]

# Join trips to routes
route_trips = trips_simple.merge(
    routes_simple,
    on="route_id",
    how="left"
)

print("Joined routes and trips:")
print(route_trips.head())

# Count number of trips per route
trip_counts = (
    route_trips
    .groupby("route_long_name")
    .size()
    .reset_index(name="number_of_trips")
    .sort_values(by="number_of_trips", ascending=False)
)

print("Trips per route:")
print(trip_counts.head(10))

# Sort stop_times so first stop is first
stop_times_sorted = stop_times.sort_values(
    by=["trip_id", "stop_sequence"]
)

# Get first stop time for each trip
first_stop_times = stop_times_sorted.groupby("trip_id").first().reset_index()

print("First stop times:")
print(first_stop_times[["trip_id", "arrival_time"]].head())


def time_to_minutes(time_str):
    hours, minutes, seconds = time_str.split(":")
    return int(hours) * 60 + int(minutes)
first_stop_times["arrival_minutes"] = (
    first_stop_times["arrival_time"]
    .apply(time_to_minutes)
)

print(first_stop_times[["arrival_time", "arrival_minutes"]].head())

# Attach arrival times to route trips
route_trips_time = route_trips.merge(
    first_stop_times[["trip_id", "arrival_minutes"]],
    on="trip_id",
    how="left"
)

print(route_trips_time.head())

def time_bucket(minutes):
    if minutes < 360:
        return "Early Morning"
    elif minutes < 600:
        return "Morning Peak"
    elif minutes < 900:
        return "Midday"
    elif minutes < 1140:
        return "Evening Peak"
    else:
        return "Night"


route_trips_time["time_of_day"] = (
    route_trips_time["arrival_minutes"]
    .apply(time_bucket)
)

print(route_trips_time[["route_long_name", "arrival_minutes", "time_of_day"]].head())


calendar_simple = calendar[["service_id", "date", "exception_type"]]
print(calendar_simple.head())

trips_service = trips[["trip_id", "route_id", "service_id"]]

route_trips_service = route_trips.merge(
    trips_service,
    on=["trip_id", "route_id"],
    how="left"
)

print(route_trips_service.head())

import datetime

def is_weekend(date_val):
    date_str = str(date_val)
    date = datetime.datetime.strptime(date_str, "%Y%m%d")
    return date.weekday() >= 5



calendar_simple["is_weekend"] = calendar_simple["date"].apply(is_weekend)
print(calendar_simple[["date", "is_weekend"]].head(10))


trip_calendar = route_trips_service.merge(
    calendar_simple,
    on="service_id",
    how="left"
)

print(trip_calendar.head())


service_summary = (
    trip_calendar
    .groupby(["route_long_name", "is_weekend"])
    .size()
    .reset_index(name="trip_count")
)

print(service_summary.head(10))



calendar_simple = calendar[["service_id", "date"]].copy()


calendar_simple["is_weekend"] = calendar_simple["date"].apply(is_weekend)

print(calendar_simple.head())

print(trips.columns)

trips_service = trips[["trip_id", "route_id", "service_id"]]

trip_calendar = trips_service.merge(
    calendar_simple,
    on="service_id",
    how="left"
)

print(trip_calendar.head())


trip_calendar = trip_calendar.merge(
    routes_simple,
    on="route_id",
    how="left"
)

print(trip_calendar.head())


service_counts = (
    trip_calendar
    .groupby(["route_long_name", "is_weekend"])
    .size()
    .reset_index(name="trip_count")
)

print(service_counts.head(10))


service_counts["day_type"] = service_counts["is_weekend"].map(
    {False: "Weekday", True: "Weekend"}
)

service_counts = service_counts.drop(columns="is_weekend")

print(service_counts.head(10))


trip_calendar_time = trip_calendar.merge(
    route_trips_time[["trip_id", "arrival_minutes", "time_of_day"]],
    on="trip_id",
    how="left"
)

print(trip_calendar_time.head())

trip_calendar_time["day_type"] = trip_calendar_time["is_weekend"].map(
    {False: "Weekday", True: "Weekend"}
)


peak_counts = (
    trip_calendar_time
    .groupby(["route_long_name", "day_type", "time_of_day"])
    .size()
    .reset_index(name="trip_count")
)

print(peak_counts.head(15))


morning_peak = peak_counts[
    peak_counts["time_of_day"] == "Morning Peak"
].sort_values(by="trip_count", ascending=False)

print(morning_peak.head(10))


print(peak_counts["time_of_day"].value_counts())

weekday_morning = peak_counts[
    (peak_counts["day_type"] == "Weekday") &
    (peak_counts["time_of_day"] == "Morning Peak")
].sort_values(by="trip_count", ascending=False)

print(weekday_morning.head(10))

import matplotlib.pyplot as plt

top10 = weekday_morning.head(10)

plt.figure(figsize=(10, 6))
plt.barh(
    top10["route_long_name"],
    top10["trip_count"]
)
plt.xlabel("Number of Trips")
plt.ylabel("Rail Route")
plt.title("NJ Transit Weekday Morning Peak Service by Route")
plt.gca().invert_yaxis()
plt.tight_layout()

output_path = BASE_DIR / "outputs" / "charts" / "weekday_morning_peak.png"
plt.savefig(output_path)
plt.show()

weekday_weekend = (
    trip_calendar_time
    .groupby(["route_long_name", "day_type"])
    .size()
    .reset_index(name="trip_count")
)

print(weekday_weekend.head())

top_routes = (
    weekday_weekend
    .groupby("route_long_name")["trip_count"]
    .sum()
    .sort_values(ascending=False)
    .head(5)
    .index
)

comparison = weekday_weekend[
    weekday_weekend["route_long_name"].isin(top_routes)
]

plt.figure(figsize=(10, 6))

for route in top_routes:
    data = comparison[comparison["route_long_name"] == route]
    plt.bar(
        data["day_type"],
        data["trip_count"],
        label=route
    )

plt.xlabel("Day Type")
plt.ylabel("Number of Trips")
plt.title("Weekday vs Weekend Service by Route")
plt.legend()
plt.tight_layout()

output_path = BASE_DIR / "outputs" / "charts" / "weekday_vs_weekend.png"
plt.savefig(output_path)
plt.show()

