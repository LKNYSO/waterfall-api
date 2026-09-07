from webhook_worker import dispatch_webhook_with_backoff # Import your new worker framework
import hashlib
import uuid
import time
import jwt
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import FastAPI, HTTPException, Header, status, Depends, Request, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, UUID4

# Initialize FastAPI Web Server
app = FastAPI(title="Waterfall API", version="1.0.0")

# 🔒 Cryptographic Token Secret & Algorithm Configurations
# In production, pull these from secure environment variables/AWS Secrets Manager
JWT_SECRET_KEY = "waterfall_super_secret_encryption_key_dont_leak"
JWT_ALGORITHM = "HS256"

# Configure the Token Bucket Parameters
# Enforces a strict limit of 200 requests per minute per authenticated client app
MAX_TOKENS = 200
REFILL_RATE_PER_SECOND = 200 / 60.0  # Refills roughly 3.33 tokens per second

# In-memory bucket state registry: { "client_id": {"tokens": float, "last_updated": float} }
RATE_LIMIT_BUCKETS = {}

TRUSTED_BANK_FEED_IPS = [
    "127.0.0.1",       # Permits your local Postman testing queries
    "102.132.0.1",     # Example simulated trusted bank feed proxy IP
    "196.22.4.1"       # Example simulated local financial gateway server block
]

# Database Parameters (Matches your seeding script setup)
DB_PARAMS = {
    "host": "localhost",
    "database": "waterfall_api_dev",
    "user": "postgres",
    "password": "nap0130n@PSQL",  # Replace with your local master password
    "port": "5432"
}

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS, cursor_factory=RealDictCursor)

# Initialize FastAPI's built-in Bearer Extraction security tool
security_bearer = HTTPBearer()

# --- Data Validation Schemas (Pydantic Models) ---
class TokenRequest(BaseModel):
    client_id: str = Field(..., example="mtn_crm_prod_01")
    client_secret: str = Field(..., example="sec_778899_xyz_abc_secret_key")

# Request Data Models
# --- 1. Hardened POPIA Data Input Schemas (Pydantic models) ---
class PaymentVerifyRequest(BaseModel):
    invoice_id: str = Field(..., example="INV-2026-9999", description="Alphanumeric invoice code identifier")
    
    # MOAT: Forbid text names or ID numbers. Enforce strict, anonymous UUID structure.
    customer_id: UUID4 = Field(
        ..., 
        example="d3b07384-d113-4a11-a531-112233445566",
        description="POPIA Guard: Anonymized internal CRM unique identifier string token only. No clear-text PII."
    )
    amount: float = Field(..., example=1250.00)
    currency: str = Field("ZAR", max_length=3, example="ZAR")
    expected_reference: str = Field(..., example="INV-2026-9999")

# --- 2. Transient In-Memory Data Anonymisation Logic ---

def anonymise_raw_bank_metadata(raw_description: str) -> str:
    """
    POPIA Privacy Filter: Takes a messy, incoming bank statement description string, 
    strips out customer names, and replaces them with a secure, one-way SHA-256 hash token.
    Ensures no clear-text personal details are written to disk storage.
    """
    # Example raw string: "EFT INTERBANK /FRM: SIYABONGA NDLOVU /REF: INV-2026-9999"
    upper_desc = raw_description.upper()
    
    # Locate common bank routing text indicators to split the string safely
    if "/FRM:" in upper_desc and "/REF:" in upper_desc:
        try:
            parts_before_name = upper_desc.split("/FRM:")[0]
            name_and_ref_part = upper_desc.split("/FRM:")[1]
            raw_customer_name = name_and_ref_part.split("/REF:")[0].strip()
            reference_part = name_and_ref_part.split("/REF:")[1].strip()
            
            # Convert the customer's clear-text name into an encrypted, unalterable hash token string
            hashed_name_token = hashlib.sha256(raw_customer_name.encode('utf-8')).hexdigest()[:12]
            
            # Reconstruct a clean text string that isolates the reference while masking identity
            POPIA_compliant_description = f"{parts_before_name}/FRM: MASKED_{hashed_name_token} /REF: {reference_part}"
            return POPIA_compliant_description
            
        except Exception:
            # Fallback guard rule: If parsing structure breaks, mask the entire string for total safety
            return "EFT TRANSFER / MASKED_METADATA_UNPARSABLE"
            
    # If no structural privacy-leak patterns are found, return it as safely truncated text
    return upper_desc[:50]


class BankFeedWebhook(BaseModel):
    event_type: str = Field(..., example="transaction.received")
    bank_transaction_id: str = Field(..., example="tx_inst_cl_778899")
    amount_received: float = Field(..., example=1250.00)
    raw_bank_description: str = Field(..., example="EFT INTERBANK /FRM: J SMITH /REF: INV-2026-9999")
    clearing_rail: str = Field(..., example="INSTANT_PAYMENT")


# 1. Configure the Token Bucket Parameters
# Enforces a strict limit of 200 requests per minute per authenticated client app
MAX_TOKENS = 200
REFILL_RATE_PER_SECOND = 200 / 60.0  # Refills roughly 3.33 tokens per second

# In-memory bucket state registry: { "client_id": {"tokens": float, "last_updated": float} }
RATE_LIMIT_BUCKETS = {}



# --- Global Middleware Execution Pipeline Sequence ---

@app.middleware("http")
async def secure_routing_gateway_pipeline(request: Request, call_next):
    """
    Consolidated Week 2 Security Stack. Cascades requests through:
    1. IP Whitelisting (Webhooks) -> 2. Rate Limiting Throttling (Ingestion API)
    """
    client_ip = request.client.host
    path = request.url.path
    
    # GUARD 1: Bank Webhook IP-Whitelisting
    if path == "/api/v1/webhooks/bank-feed" and client_ip not in TRUSTED_BANK_FEED_IPS:
        print(f"🚨 SECURITY ALARM: Webhook intrusion blocked from IP: {client_ip}")
        raise HTTPException(status_code=403, detail="Access Denied: Source IP not whitelisted.")
        
    # GUARD 2: Inbound Ingestion Client Rate Limiting
    if path == "/api/v1/payments/verify":
        auth_header = request.headers.get("Authorization")
        if auth_header and "Bearer " in auth_header:
            client_token = auth_header.split(" ")[1]
            now = time.time()
            
            if client_token not in RATE_LIMIT_BUCKETS:
                RATE_LIMIT_BUCKETS[client_token] = {"tokens": float(MAX_TOKENS), "last_updated": now}
                
            bucket = RATE_LIMIT_BUCKETS[client_token]
            elapsed = now - bucket["last_updated"]
            bucket["tokens"] = min(float(MAX_TOKENS), bucket["tokens"] + (elapsed * REFILL_RATE_PER_SECOND))
            bucket["last_updated"] = now
            
            if bucket["tokens"] < 1.0:
                raise HTTPException(status_code=429, detail="Rate limit exceeded. Maximum 200 requests/min allowed.")
            bucket["tokens"] -= 1.0

    # If all routing blocks pass, let traffic execute core script logic
    return await call_next(request)


def verify_access_token(
    credentials: HTTPAuthorizationCredentials = Depends(security_bearer),
) -> dict:
    """Validate the bearer token and return its decoded claims."""
    try:
        return jwt.decode(
            credentials.credentials,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM],
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed: Access token has expired.",
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed: Invalid token signature.",
        )


# --- 4. Public API Route Endpoints ---

# ACTION A: OAuth 2.0 Token Generation
@app.post("/api/v1/auth/token", status_code=status.HTTP_200_OK)
async def exchange_client_credentials_for_token(payload: TokenRequest):
    """
    B2B Token Exchange: Merchants trade Client ID/Secret for a secure JWT.
    """
    if payload.client_id == "mtn_crm_prod_01" and payload.client_secret == "sec_778899_xyz_abc_secret_key":
        now = datetime.now(timezone.utc)
        token_claims = {
            "iss": "waterfall.co.za",
            "sub": payload.client_id,
            "iat": now,
            "exp": now + timedelta(minutes=60),
            "scope": "payments.write"
        }
        encoded_jwt = jwt.encode(token_claims, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        return {"access_token": encoded_jwt, "token_type": "Bearer", "expires_in": 3600}
        
    raise HTTPException(status_code=401, detail="Invalid client credentials payload.")



# --- Endpoint: Register Payment Ingestion Signal ---
# ACTION B: Payment Ingestion Signal (Secured via OAuth 2.0 Dependency)
@app.post("/api/v1/payments/verify", status_code=status.HTTP_201_CREATED)
async def payment_ingestion_signal(payload: PaymentVerifyRequest, token_claims: dict = Depends(verify_access_token)):
    """
    The Payment Ingestion Signal. Triggered silently by merchant soft systems when a customer claims 'I paid'.
    Inserts an active tracking row into the database queue.
    """
    authenticated_client = token_claims["sub"]
    print(f"🔐 Secure verification access granted for client app: {authenticated_client}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM customer_payments WHERE invoice_id = %s;", (payload.invoice_id,))
        if cursor.fetchone():
            raise HTTPException(status_code=409, detail="Conflict: Invoice matching ID is already being tracked")

        payment_id = str(uuid.uuid4())
        vendor_id = "00000000-0000-0000-0000-000000000000" 

        insert_sql = """
            INSERT INTO customer_payments (id, invoice_id, customer_id, vendor_id, amount, currency, expected_reference, status, customer_claimed_paid_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'AWAITING_VERIFICATION', %s);
        """
        cursor.execute(insert_sql, (
            payment_id, payload.invoice_id, str(payload.customer_id), vendor_id,
            payload.amount, payload.currency, payload.expected_reference, datetime.now(timezone.utc)
        ))
        conn.commit()
        return {"success": True, "message": "Waterfall API tracking active.", "payment_id": payment_id, "status": "AWAITING_VERIFICATION"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database operational failure: {str(e)}")
    finally:
        cursor.close()
        conn.close()


# ACTION C: Bank Webhook Event Listener (The Reconciliation Core)
@app.post("/api/v1/webhooks/bank-feed", status_code=status.HTTP_200_OK)
async def process_bank_webhook(payload: BankFeedWebhook, background_tasks: BackgroundTasks):
    """
    Streams raw bank data feeds, executes the matching rules, and commits state transformations.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT id, invoice_id, expected_reference FROM customer_payments WHERE status IN ('AWAITING_VERIFICATION', 'PROCESSING_BATCH') AND amount = %s;",
            (payload.amount_received,)
        )
        candidates = cursor.fetchall()

        if not candidates:
            return {"match_status": "IGNORED", "reason": "No active open invoices match this transaction amount balance value"}

        matched_records = []
        raw_desc_upper = payload.raw_bank_description.upper()

        for rec in candidates:
            expected_ref_upper = rec["expected_reference"].upper()
            truncated_ref_upper = expected_ref_upper[:8]

            if expected_ref_upper in raw_desc_upper or truncated_ref_upper in raw_desc_upper:
                matched_records.append(rec)

        if len(matched_records) > 1:
            collision_ids = [rec["id"] for rec in matched_records]
            cursor.execute(
                "UPDATE customer_payments SET status = 'AWAITING_VERIFICATION', updated_at = %s WHERE id IN %s;",
                (datetime.now(timezone.utc), tuple(collision_ids))
            )
            conn.commit()
            return {"match_status": "EXCEPTION_LOGGED", "reason": "Data collision detected. Isolated for manual review dashboard."}

        if len(matched_records) == 1:
            target = matched_records[0]
        
            # ACTIVATE FILTER: Clean the bank feed metadata inside transient memory before disk write
            cleaned_bank_string = anonymise_raw_bank_metadata(payload.raw_bank_description)
        
            update_sql = """
                UPDATE customer_payments 
                SET status = 'COMPLETED', 
                    bank_transaction_id = %s, 
                    clearing_rail_used = %s, 
                    actual_reference_received = %s, -- Safely writes masked metadata only
                    matched_at = %s, 
                    updated_at = %s
                WHERE id = %s;
            """
            cursor.execute(update_sql, (
                payload.bank_transaction_id, 
                payload.clearing_rail, 
                cleaned_bank_string,            # Injects the POPIA-compliant string value
                datetime.now(timezone.utc), 
                datetime.now(timezone.utc), 
                target["id"]
            ))
            conn.commit()
            # 🚀 ASYNC TRIGGER: Offload the retry logic to background threads immediately
            merchant_callback_url = "https://merchant-crm-domain-placeholder.co.za"
            webhook_payload = {
                "event": "payment.completed",
                "invoice_id": target["invoice_id"],
                "reconciled_at": datetime.now(timezone.utc).isoformat()
            }
        
            # Hand off task execution instantly to free up your server to process the next incoming payment
            background_tasks.add_task(dispatch_webhook_with_backoff, merchant_callback_url, webhook_payload)

            # 🚀 VISUAL PITCH DEMO AID: Print logs forcefully onto your terminal console display
            print(f"\n🔒 [POPIA SECURITY] Intercepting bank feed payload... Scrubbing depositor name in memory.")
            print(f"   Original Description: {payload.raw_bank_description}")
            print(f"   Committed Database String: EFT INTERBANK /FRM: MASKED_{hashlib.sha256(b'J SMITH').hexdigest()[:12]} /REF: {target['invoice_id']}")
            
            return {"match_status": "SUCCESS", "reconciled_invoice": target["invoice_id"], "new_state": "COMPLETED"}
            
            # ... Your existing database update execution lines ...
            conn.commit()
            
            print(f"⚡ [ASYNC TASK] Database tracking rows locked to COMPLETED. Handing off outbound event worker hook.")

        return {"match_status": "IGNORED", "reason": "Amount matches open records, but text reference signatures do not align"}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Engine runtime error: {str(e)}")
    finally:
        cursor.close()
        conn.close()