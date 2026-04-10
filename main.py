from fastapi import FastAPI, Request, BackgroundTasks
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import uuid
import time

def append_with_retry(rows, max_retries=3):
    for attempt in range(max_retries):
        try:
            sheet.append_rows(rows)
            print(f"✅ Inserted {len(rows)} rows")
            return
        except Exception as e:
            print(f"❌ Attempt {attempt+1} failed:", e)
            time.sleep(1)

    print("🚨 FAILED after retries")

app = FastAPI()

SECRET_KEY = "my_super_secret_123"

# 🔐 scope
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
                                            # SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# 👉 load credentials
creds = Credentials.from_service_account_file(
    "appsheet-webhook-b99538089c56.json",  # đổi tên nếu file bạn khác
    scopes=SCOPES
)

client = gspread.authorize(creds)

# 👉 mở Google Sheet
sheet = client.open("student_table").worksheet("Student_ID_data")

# 🔥 CACHE (global)
existing_cache = set()
def load_cache():
    global existing_cache
    records = sheet.get_all_records()
    existing_cache = set()

    for r in records:
        existing_cache.add((r["Student_ID"], r["Teacher_ID"],r["Shift_2h"], r["DateTime"]))

    print(f"✅ Cache loaded: {len(existing_cache)} records")


# 👉 load cache khi start server
load_cache()
# 🔥 LOGIC XỬ LÝ (background)
def process_students(data):
    global existing_cache

    student_list = data.get("id_student_list", "")
    students = student_list.split(",")

    teacher_id = data.get("teacher_id")
    shift_2h = data.get("shift_2h")
    datetime_check = data.get("datetime_check")
    
    rows = []

    for student_id in students:
        key = (student_id, teacher_id, shift_2h,datetime_check)

        if key in existing_cache:
            print(f"⏩ Skip duplicate: {student_id}")
            continue

        row = [
            "STD_" + uuid.uuid4().hex[:8],
            student_id.strip(),
            teacher_id,
            shift_2h,
            datetime_check,
            datetime.now().isoformat()
        ]

        rows.append(row)
        existing_cache.add(key)  # 🔥 update cache ngay

    if rows:
        # sheet.append_rows(rows)
        append_with_retry(rows)
        print(f"✅ Inserted {len(rows)} rows")

@app.get("/")
def home():
    return {"status":"ok"}
@app.post("/webhook")
async def webhook(request: Request, bg: BackgroundTasks):
    data = await request.json()

        # 🔐 check secret
    if data.get("secret") != SECRET_KEY:
        return {"error": "unauthorized"}
    
    bg.add_task(process_students, data)
    print("RAW DATA:", data)

