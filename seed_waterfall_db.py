import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import uuid
from datetime import datetime, timedelta, timezone

# #1. Cloud Database Connection Configuration (Supabase Instance Pooler)
# Note: Punctuation characters are URL-encoded securely to prevent URI syntax string crashes [B]
SUPABASE_URI = "postgresql://postgres.faonspoqastaempflpee:mp3EjNdA9A3qFv0e@aws-0-eu-central-1.pooler.supabase.com:5432/postgres"


def setup_and_seed_db():
    print("🚀 Initializing Waterfall API Supabase Setup...")

    # 🔒 Clear out local scope gaps and force the engine to grab your line 8 cloud parameters
    global SUPABASE_URI
    
    # Connect to default postgres database to create our development DB if it doesn't exist
    conn = psycopg2.connect(SUPABASE_URI)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    
    # 2. Execute Table Architecture Setup
    print("🛠️ Building schema and creating indexes...")
    
    schema_sql = """
    -- Create the Payment Verification Status ENUM
    DO $$ 
    BEGIN 
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'payment_verification_status') THEN 
            CREATE TYPE payment_verification_status AS ENUM ('UNPAID', 'AWAITING_VERIFICATION', 'PROCESSING_BATCH', 'MATCH_FOUND', 'COMPLETED', 'FAILED_UNVERIFIED'); 
        END IF; 
    END $$;

        'UNPAID',
        'AWAITING_VERIFICATION',
        'PROCESSING_BATCH',
        'MATCH_FOUND',
        'COMPLETED',
        'FAILED_UNVERIFIED'
    );

    -- Create the Main Core Payments Table
    CREATE TABLE customer_payments (
        id UUID PRIMARY KEY,
        invoice_id VARCHAR(50) NOT NULL UNIQUE,
        customer_id UUID NOT NULL,
        vendor_id UUID NOT NULL,
        amount NUMERIC(12, 2) NOT NULL,
        currency CHAR(3) NOT NULL DEFAULT 'ZAR',
        expected_reference VARCHAR(50) NOT NULL,
        status payment_verification_status NOT NULL DEFAULT 'UNPAID',
        matched_at TIMESTAMP WITH TIME ZONE,
        bank_transaction_id VARCHAR(100),
        clearing_rail_used VARCHAR(30),
        actual_reference_received TEXT,
        customer_claimed_paid_at TIMESTAMP WITH TIME ZONE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    -- Create Highly Optimized Partial Indexes for the Waterfall Poller Queue
    CREATE INDEX idx_payments_verification_queue 
    ON customer_payments (status, amount) 
    WHERE status IN ('AWAITING_VERIFICATION', 'PROCESSING_BATCH');

    CREATE INDEX idx_payments_timeout_check 
    ON customer_payments (status, customer_claimed_paid_at) 
    WHERE status = 'PROCESSING_BATCH';
    """
    cursor.execute(schema_sql)
    conn.commit()

    # 3. Seed Realistic South African Payment Scenarios
    print("🌱 Seeding test scenarios into 'customer_payments' table...")
    
    now = datetime.now(timezone.utc)
    vendor_uuid = str(uuid.uuid4())
    
    # Define a clean list of explicit test cases representing real-world edge cases
    test_cases = [
        # Scenario A: Clean instant payment awaiting standard webhook execution
        {
            "id": str(uuid.uuid4()), "invoice_id": "INV-2026-0001", "customer_id": str(uuid.uuid4()), "vendor_id": vendor_uuid,
            "amount": 1250.00, "expected_reference": "INV-2026-0001", "status": "AWAITING_VERIFICATION",
            "customer_claimed_paid_at": now - timedelta(minutes=2)
        },
        # Scenario B: Trapped on a slow traditional batch rail. Needs fuzzy match evaluation
        {
            "id": str(uuid.uuid4()), "invoice_id": "INV-2026-8894", "customer_id": str(uuid.uuid4()), "vendor_id": vendor_uuid,
            "amount": 4200.50, "expected_reference": "INV-2026-8894", "status": "PROCESSING_BATCH",
            "customer_claimed_paid_at": now - timedelta(hours=4)
        },
        # Scenario C: Potential Collision Case 1 (Identical amount and overlapping reference string base)
        {
            "id": str(uuid.uuid4()), "invoice_id": "INV-2026-9001", "customer_id": str(uuid.uuid4()), "vendor_id": vendor_uuid,
            "amount": 8500.00, "expected_reference": "INV-2026-9001", "status": "PROCESSING_BATCH",
            "customer_claimed_paid_at": now - timedelta(hours=6)
        },
        # Scenario D: Potential Collision Case 2 (The overlapping collision twin)
        {
            "id": str(uuid.uuid4()), "invoice_id": "INV-2026-9002", "customer_id": str(uuid.uuid4()), "vendor_id": vendor_uuid,
            "amount": 8500.00, "expected_reference": "INV-2026-9002", "status": "PROCESSING_BATCH",
            "customer_claimed_paid_at": now - timedelta(hours=6)
        },
        # Scenario E: Aged tracking record that should trigger an automated poller timeout drop
        {
            "id": str(uuid.uuid4()), "invoice_id": "INV-2026-0099", "customer_id": str(uuid.uuid4()), "vendor_id": vendor_uuid,
            "amount": 350.00, "expected_reference": "INV-2026-0099", "status": "PROCESSING_BATCH",
            "customer_claimed_paid_at": now - timedelta(days=3) # Older than the 48-hour limit
        }
    ]

    # Generate 95 more generic data rows to bring total sample pool depth up to 100 entries
    for i in range(3, 98):
        inv_num = f"INV-2026-05{i:02d}"
        test_cases.append({
            "id": str(uuid.uuid4()), "invoice_id": inv_num, "customer_id": str(uuid.uuid4()), "vendor_id": vendor_uuid,
            "amount": float(100 * i), "expected_reference": inv_num, "status": "UNPAID",
            "customer_claimed_paid_at": None
        })

    # Insert loop processing payload batches securely using parameterized queries
    insert_sql = """
    INSERT INTO customer_payments (id, invoice_id, customer_id, vendor_id, amount, expected_reference, status, customer_claimed_paid_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    for case in test_cases:
        cursor.execute(insert_sql, (
            case["id"], case["invoice_id"], case["customer_id"], case["vendor_id"],
            case["amount"], case["expected_reference"], case["status"], case["customer_claimed_paid_at"]
        ))
        
    conn.commit()
    
    # 4. Verify Seeding Matrix
    cursor.execute("SELECT status, count(*) FROM customer_payments GROUP BY status;")
    summary = cursor.fetchall()
    
    print("\n📊 Database Seeding Operational Summary Rows:")
    for status, count in summary:
        print(f" • Status Group [{status}]: {count} records loaded.")
        
    cursor.close()
    conn.close()
    print("\n🎉 Success! Waterfall API environment is fully prepared for test routing.")


    

# --- Add this execution segment right inside your seed_waterfall_db.py script ---
def patch_cloud_enum_types():
    conn = psycopg2.connect(SUPABASE_URI)
    cursor = conn.cursor()
    try:
        print("⏳ [PATCH ROUTINE] Injecting 'COMPLETED' state parameter into live Supabase cluster enums...")
        # Ignore errors if the value already exists by catching exceptions gracefully
        cursor.execute("ALTER TYPE payment_verification_status ADD VALUE IF NOT EXISTS 'COMPLETED';")
        conn.commit()
        print("✅ [PATCH SUCCESS] Cloud database types are perfectly synchronized!")
    except Exception as e:
        conn.rollback()
        print(f"⚠️ Patch trace warning: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    #Supabase are already successfully created, we don't need setup_and_seed_db()
    #setup_and_seed_db()#
    patch_cloud_enum_types()
