import argparse
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

DB_HOST = "localhost"
DB_NAME = "waterfall_api_dev"
DB_USER = "postgres"
DB_PASSWORD = "nap0130n@PSQL"  # Replace with your local master password
DB_PORT = "5432"

SIMULATION_RESET_INVOICE_IDS = [
    "INV-2026-0001",
    "INV-2026-8894",
    "INV-2026-9001",
    "INV-2026-9002",
    "INV-2026-0099",
]


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
    )


def reset_simulation_data():
    """Reset the sample invoices to their original open states before a fresh simulation run."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        reset_time = datetime.now(timezone.utc)
        cursor.execute(
            """
            UPDATE customer_payments
            SET status = CASE invoice_id
                    WHEN 'INV-2026-0001' THEN 'AWAITING_VERIFICATION'::payment_verification_status
                    WHEN 'INV-2026-8894' THEN 'PROCESSING_BATCH'::payment_verification_status
                    WHEN 'INV-2026-9001' THEN 'PROCESSING_BATCH'::payment_verification_status
                    WHEN 'INV-2026-9002' THEN 'PROCESSING_BATCH'::payment_verification_status
                    WHEN 'INV-2026-0099' THEN 'PROCESSING_BATCH'::payment_verification_status
                    ELSE 'UNPAID'::payment_verification_status
                END,
                bank_transaction_id = NULL,
                clearing_rail_used = NULL,
                actual_reference_received = NULL,
                matched_at = NULL,
                updated_at = %s
            WHERE invoice_id IN %s;
            """,
            (reset_time, tuple(SIMULATION_RESET_INVOICE_IDS)),
        )
        conn.commit()
        print("🔄 Simulation data reset to initial open states.")
    except Exception as e:
        conn.rollback()
        print(f"🚨 Reset failed: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


def process_incoming_bank_feed_line(bank_line):
    """
    Core Waterfall API matching engine.
    Parses a single line item from a bank feed webhook or statement file,
    and intelligently reconciles it against the customer_payments ledger.
    """
    amount = bank_line["amount_received"]
    raw_desc = bank_line["raw_bank_description"].strip()
    rail_used = bank_line["clearing_rail"]
    bank_tx_id = bank_line["bank_transaction_id"]
    
    print(f"\n🔍 Processing incoming transaction {bank_tx_id} | Amount: R{amount:.2f} | Rail: {rail_used}")
    print(f"   Raw Text: '{raw_desc}'")

    conn = get_db_connection()
    # RealDictCursor allows us to access columns by string names like a dictionary
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # 1. Query the database using the optimized partial index for rows waiting for verification
        query_candidates = """
            SELECT id, invoice_id, expected_reference, status 
            FROM customer_payments
            WHERE status IN ('AWAITING_VERIFICATION', 'PROCESSING_BATCH')
              AND amount = %s;
        """
        cursor.execute(query_candidates, (amount,))
        candidates = cursor.fetchall()

        if not candidates:
            print("   ❌ Match Status: IGNORED (No open invoices exist for this exact amount)")
            return "IGNORED_NO_AMOUNT_MATCH"

        # 2. Filter candidates based on string matching strategies
        matched_records = []
        raw_desc_upper = raw_desc.upper()

        for record in candidates:
            expected_ref_upper = record["expected_reference"].upper()
            # Handle legacy rails character truncation by checking the first 8 characters
            truncated_ref_upper = expected_ref_upper[:8]

            if expected_ref_upper in raw_desc_upper:
                matched_records.append((record, "EXACT_REF_MATCH"))
            elif truncated_ref_upper in raw_desc_upper:
                matched_records.append((record, "TRUNCATED_REF_MATCH"))

        # 3. Apply Waterfall State Machine Rules based on match results
        
        # RULE 1: Absolute Collision Catching (Double Match Protection Moat)
        if len(matched_records) > 1:
            print(f"   ⚠️ Match Status: EXCEPTION (Collision detected! {len(matched_records)} invoices match this text pattern)")
            
            # Move all colliding records to an evaluation state safely
            collision_ids = [rec[0]["id"] for rec in matched_records]
            update_collision_sql = """
                UPDATE customer_payments
                SET status = 'AWAITING_VERIFICATION', -- Retain state for manual queue visibility
                    updated_at = %s
                WHERE id IN %s;
            """
            # Passing a tuple of IDs to the SQL IN clause
            cursor.execute(update_collision_sql, (datetime.now(timezone.utc), tuple(collision_ids)))
            conn.commit()
            return "EXCEPTION_COLLISION_LOGGED"

        # RULE 2: Clean Single Match Found (Instant or Batch)
        if len(matched_records) == 1:
            matched_target, match_strategy = matched_records[0]
            print(f"   ✅ Match Status: SUCCESS ({match_strategy} found for Invoice {matched_target['invoice_id']})")

            update_success_sql = """
                UPDATE customer_payments
                SET status = 'COMPLETED',
                    bank_transaction_id = %s,
                    clearing_rail_used = %s,
                    actual_reference_received = %s,
                    matched_at = %s,
                    updated_at = %s
                WHERE id = %s;
            """
            cursor.execute(update_success_sql, (
                bank_tx_id,
                rail_used,
                raw_desc,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
                matched_target["id"]
            ))
            conn.commit()
            return "SUCCESS_COMPLETED"

        # RULE 3: Amount matches, but reference details do not align textually
        print("   ❌ Match Status: MISMATCH (Amount found, but text references do not point to this client)")
        return "IGNORED_REFERENCE_MISMATCH"

    except Exception as e:
        conn.rollback()
        print(f"   🚨 System Error running engine: {str(e)}")
        return "SYSTEM_ERROR"
    finally:
        cursor.close()
        conn.close()

# --- Week 1 Simulation Testing Script Run ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Waterfall simulation engine.")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset the seeded invoices to their starting open states before running the simulation.",
    )
    args = parser.parse_args()

    if args.reset:
        reset_simulation_data()

    print("⚡ Starting Waterfall API Simulation Run...")

    # Simulation Test A: Real-Time Instant Webhook Event (Perfect Exact Reference Match)
    mock_webhook_payload = {
        "bank_transaction_id": "tx_inst_112233",
        "amount_received": 1250.00,
        "raw_bank_description": "EFT INTERBANK /FRM: J SMITH /REF: INV-2026-0001",
        "clearing_rail": "INSTANT_PAYMENT"
    }
    process_incoming_bank_feed_line(mock_webhook_payload)

    # Simulation Test B: Traditional Batch File Entry (Truncated Text Pattern Match)
    mock_batch_payload = {
        "bank_transaction_id": "tx_batch_445566",
        "amount_received": 4200.50,
        "raw_bank_description": "STANDARD EFT REMIT / PAYEE TRUNCATED REF: INV-2026-8",
        "clearing_rail": "BATCH_EFT"
    }
    process_incoming_bank_feed_line(mock_batch_payload)

    # Simulation Test C: Critical Data Collision Pattern (Twin R8,500 Payments)
    mock_collision_payload = {
        "bank_transaction_id": "tx_batch_778899",
        "amount_received": 8500.00,
        "raw_bank_description": "BULK CONSOLIDATED EFT TRADING ACCOUNT DEPOSIT PMT / REF: INV-2026-9",
        "clearing_rail": "BATCH_EFT"
    }
    process_incoming_bank_feed_line(mock_collision_payload)
