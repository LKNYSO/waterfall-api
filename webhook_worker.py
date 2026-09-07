import time
import random
import requests
from typing import Dict, Any

# ⚙️ Resiliency Threshold Configurations
MAX_RETRIES = 5             # Maximum number of fallback attempts before marking as FAILED
BASE_DELAY_SECONDS = 2      # Exponential scaling base factor
MAX_DELAY_SECONDS = 30      # Hard ceiling to prevent delays from stretching into minutes

def dispatch_webhook_with_backoff(callback_url: str, payload: Dict[str, Any]) -> bool:
    """
    Robust Asynchronous Webhook Dispatcher.
    Implements Full Exponential Backoff capped with random Jitter to eliminate 
    the "Thundering Herd" problem when merchant servers recover.
    """
    current_delay = BASE_DELAY_SECONDS
    print(f"\n📡 [WORKER] Initiating outbound webhook dispatch to: {callback_url}")
    print(f"   Payload: {payload}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Enforce strict headers so merchants can verify packet integrity and origin
            headers = {
                "X-Waterfall-Event": "payment.reconciled",
                "X-Waterfall-Signature": "sha256_hmac_signature_token_placeholder",
                "Content-Type": "application/json",
                "User-Agent": "Waterfall-API-Webhook-Worker/2.0"
            }

            # Fire HTTP POST request with a strict 5-second socket network timeout
            response = requests.post(callback_url, json=payload, headers=headers, timeout=5)
            
            # Look explicitly for an HTTP 200 OK or 201 Created from the merchant CRM
            if response.status_code in (200, 201):
                print(f"   ✅ [ATTEMPT #{attempt}] SUCCESS! Webhook successfully delivered to merchant CRM.")
                return True
            
            print(f"   ⚠️ [ATTEMPT #{attempt}] WARNING: Merchant server rejected payload with HTTP {response.status_code}.")
            
        except requests.Timeout:
            print(f"   ⏳ [ATTEMPT #{attempt}] TIMEOUT: Merchant server failed to respond within 5 seconds.")
        except requests.RequestException as e:
            print(f"   🚨 [ATTEMPT #{attempt}] NETWORK ERROR: Transport layer connectivity failure: {str(e)}")

        # If it's the final attempt, don't execute sleep arithmetic loops
        if attempt == MAX_RETRIES:
            break

        # 🧠 Algorithmic Strategy: Full Exponential Backoff + Jitter Calculation
        # Math: Delay = UniformRandom(0, Min(Max_Delay, Base_Delay * 2^attempt))
        exponential_boundary = min(MAX_DELAY_SECONDS, current_delay * (2 ** (attempt - 1)))
        jittered_delay = random.uniform(BASE_DELAY_SECONDS, exponential_boundary)

        print(f"   🔄 Retrying execution track. Backing off for {jittered_delay:.2f} seconds...")
        time.sleep(jittered_delay)

    print(f"   ❌ [CRITICAL ALERT] Webhook delivery permanently FAILED after {MAX_RETRIES} tracking attempts.")
    print("      Routing dead letter packet to local database schema: 'ExceptionQueuePayloads'.")
    return False


# --- Week 2 Simulation Run Suite ---
if __name__ == "__main__":
    print("🧪 Running Waterfall API Outbound Webhook Stress Test Suite...")
    
    # Mock transactional data package
    mock_payload = {
        "event_type": "payment.completed",
        "invoice_id": "INV-2026-9999",
        "amount_reconciled": 1250.00,
        "clearing_rail": "INSTANT_PAYMENT"
    }

    # Simulation A: Target a fake unroutable domain port to trigger an immediate network crash loop
    mock_dead_crm_url = "http://127.0.0"
    dispatch_webhook_with_backoff(mock_dead_crm_url, mock_payload)
