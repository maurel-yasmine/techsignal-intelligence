# grant_iam.py — grant BigQuery read roles to Spark service account (Opsi A)
import json
import urllib.request
import google.auth.transport.requests
import google.oauth2.service_account

KEYFILE = r"D:\PROJECT\secret\spark-techtrend-key.json"
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

TARGET_ROLES = [
    "roles/bigquery.readSessionUser",
    "roles/bigquery.dataViewer",
]

creds = google.oauth2.service_account.Credentials.from_service_account_file(
    KEYFILE, scopes=SCOPES
)
creds.refresh(google.auth.transport.requests.Request())

with open(KEYFILE) as f:
    key = json.load(f)

PROJECT_ID = key["project_id"]
MEMBER = f"serviceAccount:{key['client_email']}"

BASE = f"https://cloudresourcemanager.googleapis.com/v1/projects/{PROJECT_ID}"


def api_call(url, method="GET", body=None):
    headers = {
        "Authorization": f"Bearer {creds.token}",
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


# --- baca policy saat ini ---
policy = api_call(f"{BASE}:getIamPolicy", "POST", {})
print(f"project: {PROJECT_ID}")
print(f"member : {MEMBER}")

# --- tambahkan binding jika belum ada ---
changed = False
for role in TARGET_ROLES:
    exists = any(
        b["role"] == role and MEMBER in b.get("members", []) for b in policy["bindings"]
    )
    if exists:
        print(f"  {role}: sudah ada, skip")
        continue
    binding = next((b for b in policy["bindings"] if b["role"] == role), None)
    if binding:
        binding["members"].append(MEMBER)
    else:
        policy["bindings"].append({"role": role, "members": [MEMBER]})
    print(f"  {role}: akan ditambahkan")
    changed = True

if not changed:
    print("SELESAI: semua role sudah ter-grant sebelumnya")
    exit(0)

# --- tulis policy ---
policy["version"] = 3
body = {"policy": policy}
try:
    api_call(f"{BASE}:setIamPolicy", "POST", body)
    print("SELESAI: IAM policy diperbarui")
except urllib.error.HTTPError as e:
    print(f"GAGAL: HTTP {e.code}: {e.read().decode()}")
    print("(opsi: pakai gcloud CLI manual atau console.cloud.google.com/IAM)")
    exit(1)
