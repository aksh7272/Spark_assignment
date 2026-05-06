import pandas as pd
import random
from io import StringIO
from google.cloud import storage

# -----------------------------
# CONFIG
# -----------------------------
BUCKET_NAME = "ak_sparkbucket"
PREFIX = "source/"

days = ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05"]

diseases = [
    ("D123", "Diabetes"),
    ("H234", "High Blood Pressure"),
    ("C345", "Cancer")
]

genders = ["M", "F"]

# -----------------------------
# GCS CLIENT
# -----------------------------
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

# -----------------------------
# GENERATION + UPLOAD
# -----------------------------
for i, day in enumerate(days):

    rows = []

    for j in range(1, 101):
        rows.append([
            f"P{i*100 + j}",
            random.randint(30, 70),
            random.choice(genders),
            random.choice(diseases)[0],
            random.choice(diseases)[1],
            day
        ])

    df = pd.DataFrame(rows, columns=[
        "patient_id",
        "age",
        "gender",
        "diagnosis_code",
        "diagnosis_description",
        "diagnosis_date"
    ])

    # Convert to CSV in memory (NO local file)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    file_name = f"health_data_{day.replace('-', '')}.csv"
    blob_path = f"{PREFIX}{file_name}"

    # Upload to GCS
    blob = bucket.blob(blob_path)
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    print(f"Uploaded: gs://{BUCKET_NAME}/{blob_path}")