# ===========================
# Fact_Visits Synthetic Data Generator
# - Creates 50-row sample CSV
# - Creates 500,000-row CSV in chunks
# Schema: flattened visit + dimension attributes
# Requires: pip install faker pandas
# ===========================

import os
import csv
import math
import random
from datetime import datetime, timedelta, date
from faker import Faker
import pandas as pd

# ---------------------------
# Config
# ---------------------------
SEED = 42
SAMPLE_ROWS = 50
TOTAL_ROWS = 500_000           # 5 lakh
CHUNK_SIZE = 50_000            # writes in 10 chunks
SAMPLE_CSV = "fact_visits_sample_50.csv"
FULL_CSV = "fact_visits_500k.csv"

# Business rules
PATIENT_REPEAT_RATE = 0.0001   # 0.01% of rows should be repeated patients
NUM_HOSPITALS = 30
NUM_DEPARTMENTS = 10
NUM_DIAGNOSES = 10
NUM_DOCTORS = 100
NUM_INSURERS = 20

# ---------------------------
# Faker & Random seeding
# ---------------------------
random.seed(SEED)
fake = Faker("en_IN")  # Indian-flavored data by default, still can create global addresses
Faker.seed(SEED)

# ---------------------------
# Helpers
# ---------------------------
def rand_time(h_start, h_end):
    """Return time string HH:MM within [h_start, h_end)."""
    h = random.randint(h_start, h_end - 1)
    m = random.choice([0, 15, 30, 45])
    return f"{h:02d}:{m:02d}"

def calc_age(dob: date, on_date: date) -> int:
    years = on_date.year - dob.year - ((on_date.month, on_date.day) < (dob.month, dob.day))
    return max(0, years)

# ---------------------------
# Fixed reference/value pools
# (stable ID -> stable attributes)
# ---------------------------

# Hospitals (30)
hospital_types = ["General", "Specialty"]
accreditations = ["Level A", "Level B", "Level C"]
indian_states = ["Maharashtra", "Karnataka", "Delhi", "Gujarat", "Tamil Nadu",
                 "Telangana", "West Bengal", "Rajasthan", "Uttar Pradesh", "Madhya Pradesh"]

hospital_pool = []
for i in range(1, NUM_HOSPITALS + 1):
    hid = f"H{i:03d}"
    name = f"Hospital_{i:02d}"
    htype = random.choice(hospital_types)
    accred = random.choice(accreditations)
    beds = random.randint(100, 1000)
    # Keep location stable
    h_state = random.choice(indian_states)
    h_city = fake.city()
    h_country = "India"
    h_addr = fake.street_address()
    h_postal = fake.postcode()
    hospital_pool.append({
        "hospitalid": hid,
        "hospitalname": name,
        "hospitaltype": htype,
        "accreditation_level": accred,
        "bed_capacity": beds,
        "hospital_address": h_addr,
        "hospitalstate": h_state,
        "hospitalcity": h_city,
        "hospitalcountry": h_country,
        "hospitalpostalcode": h_postal
    })

# Departments (10)
dept_names = [
    "Cardiology","Orthopedics","Neurology","Dermatology","Pediatrics",
    "ENT","Gastroenterology","Respiratory","Oncology","General Medicine"
]
dept_specialities = [
    "Heart","Bones","Brain","Skin","Children",
    "Ear Nose Throat","Digestive","Pulmonary","Cancer","Primary Care"
]
department_pool = []
for i in range(NUM_DEPARTMENTS):
    did = f"DEP{i+1:02d}"
    department_pool.append({
        "departmentid": did,
        "departmentname": dept_names[i],
        "deptspeciality": dept_specialities[i]
    })

# Diagnoses (10)
diag_categories = [
    "Infection","Neurology","Orthopedic","Cardio","Dermatology",
    "Respiratory","Gastro","Oncology","Pediatrics","General"
]
diagnosis_pool = []
for i in range(1, NUM_DIAGNOSES + 1):
    diagnosis_pool.append({
        "diagnosisid": f"DIAG{i:02d}",
        "diagnosiscode": f"ICD{i:03d}",
        "diagnosisdiscription": f"Diagnosis {i}",
        "diagnosiscategory": diag_categories[i-1]
    })

# Doctors (100) - attach them to a random hospital and department (stable)
specializations = dept_names[:]  # reuse department names as doctor specialization set
doctor_pool = []
for i in range(1, NUM_DOCTORS + 1):
    did = f"D{i:03d}"
    full_name = fake.name()
    spec = random.choice(specializations)
    phone = fake.phone_number()
    years_exp = random.randint(3, 35)
    email = f"{full_name.lower().replace(' ','.').replace(',','')}@hospital.com"
    # stable shift type preference (for display), actual working shift per visit comes from shift dimension
    shift_type = random.choice(["Morning","Evening","Night"])
    # stable assignment for reporting (optional)
    assigned_hosp = random.choice(hospital_pool)
    assigned_dept = random.choice(department_pool)
    doctor_pool.append({
        "Doctorid": did,
        "doctorfullname": full_name,
        "doctor_specialization": spec,
        "doctorcontactnumber": phone,
        "doctoryears_experienxe": years_exp,   # keep user's exact typo
        "doctoremail": email,
        "doctor_shift_type": shift_type,
        "assigned_hospitalid": assigned_hosp["hospitalid"],
        "assigned_departmentid": assigned_dept["departmentid"]
    })

# Insurance providers (20)
plan_types = ["Gold","Silver","Platinum"]
insurance_pool = []
for i in range(1, NUM_INSURERS + 1):
    insurance_pool.append({
        "insuranceproviderid": f"INS{i:03d}",
        "insuranceprovidername": f"Insurance_{i}",
        "insuranceplan_type": random.choice(plan_types),
        "coverage_percentage": random.randint(50, 90),
        "contact_number": fake.phone_number(),
        "insurance_email": f"claims{i}@insureco.com"
    })

# Shifts (3 fixed)
shift_pool = [
    {"shiftid": "S001", "shift_name": "Morning", "shiftstarttime": "08:00", "shiftendtime": "14:00"},
    {"shiftid": "S002", "shift_name": "Evening", "shiftstarttime": "14:00", "shiftendtime": "20:00"},
    {"shiftid": "S003", "shift_name": "Night",   "shiftstarttime": "20:00", "shiftendtime": "08:00"},
]

# ---------------------------
# Patient pools
# ---------------------------
# We will ensure EXACT number of repeat rows:
repeat_rows = max(1, int(TOTAL_ROWS * PATIENT_REPEAT_RATE))  # e.g., 50 for 500k
# Create a pool of repeatable patients with STABLE attributes
blood_groups = ["A+","A-","B+","B-","O+","O-","AB+","AB-"]

repeat_patients = []
for i in range(repeat_rows):
    pid = f"P{100000 + i}"  # stable, readable
    dob = fake.date_of_birth(minimum_age=1, maximum_age=95)
    full_name = fake.name()
    gender = random.choice(["M","F"])
    # Address (keep stable)
    addr = fake.street_address()
    city = fake.city()
    state = random.choice(indian_states)
    country = "India"
    pincode = fake.postcode()
    satisfaction = round(random.uniform(1, 5), 1)
    bgrp = random.choice(blood_groups)
    repeat_patients.append({
        "patientid": pid,
        "patient_fullname": full_name,
        "patientgender": gender,
        "patientdob": dob,
        "patientaddress": addr,
        "patientcity": city,
        "patientstate": state,
        "patientcountry": country,
        "patientpostalcode": pincode,
        "patient_satisfactionsxore": satisfaction,  # keep user's exact typo
        "patient_bloodgroup": bgrp
    })

# ---------------------------
# Output Columns (exact user names, including typos & duplication)
# ---------------------------
COLUMNS = [
    "visitid",
    "patientid","patient_fullname","patientgender","age","patientdob","patientaddress","patientcity","patientstate","patientcountry","patientpostalcode",
    "patient_satisfactionsxore","patient_bloodgroup","patientwaittime",
    "Doctorid","doctorfullname","doctor specialization","doctorcontactnumber","doctoryears_experienxe","doctoremail","doctor shift type",
    "departmentid","departmentname","deptspeciality","Departmentreferral","floor_number",
    "hospitalid","hospitalname","hospitaltype","accreditation_level","bed capacity","hospital address","hospitalstate","hospitalcity","hospitalstate2","hospitalcountry","hospitalpostalcode",
    "Roomid","roomnumber","roomtype","floornumber",
    "unitid","unitname",
    "shiftid","shift name","shiftstarttime","shiftendtime",
    "total billing amount","insurancecoveredamount","patient covered amount",
    "full_date",
    "diagnosisid","diagnosiscode","diagnosisdiscription","diagnosiscategory",
    "insuranceproviderid","insuranceprovidername","insuranceplan_type","coverage_percentage","contact_number","insurance email"
]

# ---------------------------
# Row factory (one visit)
# ---------------------------
def make_visit_row(visit_index: int, repeat_patient: dict | None = None) -> dict:
    """Create one flattened visit row; if repeat_patient provided, reuse it."""
    # Full date within last ~3 years
    full_date = fake.date_between(start_date="-3y", end_date="today")
    # If repeated patient, reuse fields; else generate new patient
    if repeat_patient:
        pat = repeat_patient
        age = calc_age(pat["patientdob"], full_date)
    else:
        dob = fake.date_of_birth(minimum_age=1, maximum_age=95)
        age = calc_age(dob, full_date)
        pat = {
            "patientid": f"P{200000 + visit_index}",
            "patient_fullname": fake.name(),
            "patientgender": random.choice(["M","F"]),
            "patientdob": dob,
            "patientaddress": fake.street_address(),
            "patientcity": fake.city(),
            "patientstate": random.choice(indian_states),
            "patientcountry": "India",
            "patientpostalcode": fake.postcode(),
            "patient_satisfactionsxore": round(random.uniform(1, 5), 1),
            "patient_bloodgroup": random.choice(blood_groups)
        }

    # Hospital (stable mapping)
    hosp = random.choice(hospital_pool)

    # Department
    dept = random.choice(department_pool)
    dept_referral = random.choice(["Self","Referral","Follow-up"])

    # Diagnosis
    diag = random.choice(diagnosis_pool)

    # Doctor (stable profile)
    doc = random.choice(doctor_pool)

    # Wait time and billing
    patientwaittime = random.randint(5, 120)  # minutes
    total_bill = random.randint(500, 20000)
    insurer = random.choice(insurance_pool)
    insurance_cover = int(total_bill * insurer["coverage_percentage"] / 100)
    patient_cover = max(0, total_bill - insurance_cover)

    # Room / Unit / Shift
    roomid = f"R{random.randint(1, 500):04d}"
    roomnumber = random.randint(100, 699)
    roomtype = random.choice(["ICU","General","OPD","ER","Private"])
    room_floor = random.randint(1, 7)

    unitid = f"U{random.randint(1, 200):03d}"
    unitname = random.choice(["ICU Unit","OPD Unit","Surgery Unit","Emergency Unit","Recovery Unit"])

    shift = random.choice(shift_pool)

    # Assemble row dict with EXACT column names
    row = {
        "visitid": f"V{visit_index:07d}",
        "patientid": pat["patientid"],
        "patient_fullname": pat["patient_fullname"],
        "patientgender": pat["patientgender"],
        "age": age,
        "patientdob": pat["patientdob"].isoformat(),
        "patientaddress": pat["patientaddress"],
        "patientcity": pat["patientcity"],
        "patientstate": pat["patientstate"],
        "patientcountry": pat["patientcountry"],
        "patientpostalcode": pat["patientpostalcode"],
        "patient_satisfactionsxore": pat["patient_satisfactionsxore"],
        "patient_bloodgroup": pat["patient_bloodgroup"],
        "patientwaittime": patientwaittime,

        "Doctorid": doc["Doctorid"],
        "doctorfullname": doc["doctorfullname"],
        "doctor specialization": doc["doctor_specialization"],
        "doctorcontactnumber": doc["doctorcontactnumber"],
        "doctoryears_experienxe": doc["doctoryears_experienxe"],
        "doctoremail": doc["doctoremail"],
        "doctor shift type": doc["doctor_shift_type"],

        "departmentid": dept["departmentid"],
        "departmentname": dept["departmentname"],
        "deptspeciality": dept["deptspeciality"],
        "Departmentreferral": dept_referral,
        "floor_number": random.randint(1, 7),

        "hospitalid": hosp["hospitalid"],
        "hospitalname": hosp["hospitalname"],
        "hospitaltype": hosp["hospitaltype"],
        "accreditation_level": hosp["accreditation_level"],
        "bed capacity": hosp["bed_capacity"],
        "hospital address": hosp["hospital_address"],
        "hospitalstate": hosp["hospitalstate"],
        "hospitalcity": hosp["hospitalcity"],
        # duplicate field preserved as hospitalstate2:
        "hospitalstate2": hosp["hospitalstate"],
        "hospitalcountry": hosp["hospitalcountry"],
        "hospitalpostalcode": hosp["hospitalpostalcode"],

        "Roomid": roomid,
        "roomnumber": roomnumber,
        "roomtype": roomtype,
        "floornumber": room_floor,

        "unitid": unitid,
        "unitname": unitname,

        "shiftid": shift["shiftid"],
        "shift name": shift["shift_name"],
        "shiftstarttime": shift["shiftstarttime"],
        "shiftendtime": shift["shiftendtime"],

        "total billing amount": total_bill,
        "insurancecoveredamount": insurance_cover,
        "patient covered amount": patient_cover,

        "full_date": full_date.isoformat(),

        "diagnosisid": diag["diagnosisid"],
        "diagnosiscode": diag["diagnosiscode"],
        "diagnosisdiscription": diag["diagnosisdiscription"],
        "diagnosiscategory": diag["diagnosiscategory"],

        "insuranceproviderid": insurer["insuranceproviderid"],
        "insuranceprovidername": insurer["insuranceprovidername"],
        "insuranceplan_type": insurer["insuranceplan_type"],
        "coverage_percentage": insurer["coverage_percentage"],
        "contact_number": insurer["contact_number"],
        "insurance email": insurer["insurance_email"]
    }
    return row

# ---------------------------
# Writers
# ---------------------------
def write_csv_header(path: str):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()

def append_csv_rows(path: str, rows: list[dict]):
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        for r in rows:
            w.writerow(r)

# ---------------------------
# Generate 50-row sample
# ---------------------------
def generate_sample():
    rows = []
    # choose a few repeat patients for demo
    chosen_repeat = random.sample(repeat_patients, k=min(3, len(repeat_patients)))
    for i in range(1, SAMPLE_ROWS + 1):
        # about same repeat logic scaled down
        use_repeat = (i % 10 == 0) and chosen_repeat  # every 10th row repeat one of them
        rp = random.choice(chosen_repeat) if use_repeat else None
        rows.append(make_visit_row(i, rp))
    df = pd.DataFrame(rows, columns=COLUMNS)
    df.to_csv(SAMPLE_CSV, index=False)
    print(f"✅ Wrote sample CSV: {SAMPLE_CSV} ({len(rows)} rows)")

# ---------------------------
# Generate 500k rows in chunks
# ---------------------------
def generate_full():
    # plan exact positions for repeat rows
    # pick 'repeat_rows' distinct indices in [1..TOTAL_ROWS] to be repeated-patient records
    repeat_positions = set(random.sample(range(1, TOTAL_ROWS + 1), k=repeat_rows))

    write_csv_header(FULL_CSV)
    buffer = []
    for i in range(1, TOTAL_ROWS + 1):
        rp = random.choice(repeat_patients) if i in repeat_positions else None
        buffer.append(make_visit_row(i, rp))
        if len(buffer) >= CHUNK_SIZE:
            append_csv_rows(FULL_CSV, buffer)
            buffer.clear()
            print(f"... wrote up to visit {i:,}")

    if buffer:
        append_csv_rows(FULL_CSV, buffer)

    print(f"✅ Wrote full CSV: {FULL_CSV} ({TOTAL_ROWS:,} rows)")
    print(f"ℹ️  Repeat patient rows: {repeat_rows} ({PATIENT_REPEAT_RATE*100:.4f}%)")

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    generate_sample()
    generate_full()
