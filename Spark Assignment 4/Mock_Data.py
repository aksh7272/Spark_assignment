import csv
import random
from datetime import datetime, timedelta

# Config
NUM_ROWS = 1000
OUTPUT_FILE = "ad_data.csv"

start_time = datetime(2026, 01, 01, 10, 0, 0)


def generate_row(i, base_time):
    timestamp = base_time + timedelta(seconds=i * 60)

    views = random.randint(5, 100)
    clicks = random.randint(0, views)  # clicks <= views

    # Cost roughly proportional to clicks
    cost = round(clicks * random.uniform(0.5, 5.0), 2)

    return [
        str(10000 + i),  # ad_id
        timestamp.isoformat() + "Z",  # timestamp
        clicks,
        views,
        cost
    ]


def generate_csv():
    with open(OUTPUT_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)

        # Header
        writer.writerow(["ad_id", "timestamp", "clicks", "views", "cost"])

        # Data rows
        for i in range(NUM_ROWS):
            writer.writerow(generate_row(i, start_time))

    print(f"CSV file '{OUTPUT_FILE}' with {NUM_ROWS} rows generated successfully.")


if __name__ == "__main__":
    generate_csv()