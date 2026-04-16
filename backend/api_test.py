import urllib.request
import json
import traceback

def test_api():
    base_url = "http://localhost:8000"
    endpoints = [
        "/health",
        "/api/transactions",
        "/api/metrics"
    ]
    
    print("Testing API Endpoints...")
    all_success = True
    for ep in endpoints:
        url = base_url + ep
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                print(f"[OK] {ep}: Success (Status: {resp.status})")
                if ep == "/api/transactions" and len(data) > 0:
                    # also test get single
                    t_id = data[0].get("transaction_id")
                    if t_id:
                        s_url = f"{base_url}/api/transactions/{t_id}"
                        with urllib.request.urlopen(s_url) as s_resp:
                            s_data = json.loads(s_resp.read().decode('utf-8'))
                            print(f"[OK] /api/transactions/{{id}}: Success (Status: {s_resp.status})")
        except Exception as e:
            print(f"[FAIL] {ep}: Failed - {str(e)}")
            all_success = False

    if all_success:
        print("\nAll endpoints working as intended.")
    else:
        print("\nSome endpoints failed.")

if __name__ == "__main__":
    test_api()
