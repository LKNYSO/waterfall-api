import os
import base64
import httpx
import psycopg2
from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta, timezone


router = APIRouter(prefix="/api/v1/xero", tags=["Xero Accounting Bridge"])

XERO_CLIENT_ID = os.getenv("XERO_CLIENT_ID")
XERO_CLIENT_SECRET = os.getenv("XERO_CLIENT_SECRET")

# Automatically defaults to localhost:8000 for local testing unless overridden in Render
REDIRECT_URI = os.getenv(
    "XERO_REDIRECT_URI",
    "http://localhost:8000/api/v1/xero/callback"
)

SCOPES = "offline_access accounting.transactions accounting.contacts.read"

def upsert_xero_tokens(tenant_id: str, org_name: str, access_token: str, refresh_token: str, expires_in: int = 1800):
    """Stores or updates a patron's Xero OAuth tokens in Supabase."""
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 60)  # 60s safety buffer

    conn = get_xero_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO public.xero_tenants (tenant_id, organization_name, access_token, refresh_token, expires_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id) DO UPDATE SET
                    organization_name = EXCLUDED.organization_name,
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = NOW();
                """,
                (tenant_id, org_name, access_token, refresh_token, expires_at),
            )
            conn.commit()
    finally:
        conn.close()


async def get_valid_xero_token(tenant_id: str) -> str:
    """Fetches a valid access token from Supabase, auto-refreshing if within 60s of expiry."""
    conn = get_xero_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT access_token, refresh_token, expires_at FROM public.xero_tenants WHERE tenant_id = %s AND is_active = TRUE;",
                (tenant_id,),
            )
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Xero tenant not connected.")

            access_token, refresh_token, expires_at = row

            # Return existing token immediately if still valid
            if datetime.now(timezone.utc) < expires_at:
                return access_token

            # Otherwise exchange the rotating refresh_token for a fresh pair
            creds = base64.b64encode(f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}".encode()).decode()
            async with httpx.AsyncClient() as client:
                res = await client.post(
                    "https://xero.com",
                    headers={
                        "Authorization": f"Basic {creds}",
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                    data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                )
                if res.status_code != 200:
                    raise HTTPException(status_code=401, detail="Xero token refresh failed; patron must reconnect.")
                new_tokens = res.json()

            # Persist the new access_token and rotated refresh_token immediately
            new_expiry = datetime.now(timezone.utc) + timedelta(seconds=new_tokens["expires_in"] - 60)
            cursor.execute(
                """
                UPDATE public.xero_tenants
                SET access_token = %s, refresh_token = %s, expires_at = %s, updated_at = NOW()
                WHERE tenant_id = %s;
                """,
                (new_tokens["access_token"], new_tokens["refresh_token"], new_expiry, tenant_id),
            )
            conn.commit()
            return new_tokens["access_token"]
    finally:
        conn.close()

def get_xero_db_connection():
    db_password = os.getenv("DATABASE_PASSWORD")
    if not db_password:
        raise ValueError("CRITICAL: DATABASE_PASSWORD environment variable is not set!")

    return psycopg2.connect(
        host="://supabase.com",
        port=6543,
        dbname="postgres",
        user="postgres.faonspoqastaempflpee",
        password=db_password,
        sslmode="require"
    )

@router.get("/connect")
def initiate_xero_oauth():
    """Generates the self-serve OAuth 2.0 login URL matching the active environment."""
    if not XERO_CLIENT_ID:
        raise HTTPException(status_code=500, detail="XERO_CLIENT_ID is not configured.")

    auth_url = (
        "https://login.xero.com/identity/connect/authorize"
        f"?response_type=code&client_id={XERO_CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}&scope={SCOPES}"
    )
    return {
        "authorization_url": auth_url,
        "active_redirect_uri": REDIRECT_URI
    }


@router.get("/callback")
async def xero_oauth_callback(code: str):
    """Exchanges the authorization code for an access token and Xero Tenant ID."""
    if not XERO_CLIENT_ID or not XERO_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Xero credentials are not configured.")

    creds = base64.b64encode(f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}".encode()).decode()

    async with httpx.AsyncClient() as client:
        token_res = await client.post(
            "https://identity.xero.com/connect/token",
            headers={
                "Authorization": f"Basic {creds}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI,
            },
        )
        if token_res.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail=f"Xero token exchange failed: {token_res.text}",
            )
        tokens = token_res.json()

        conn_res = await client.get(
            "https://api.xero.com/connections",
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        tenants = conn_res.json()

    if not tenants:
        raise HTTPException(status_code=400, detail="No Xero organizations were authorized.")

    primary_tenant = tenants[0]

    upsert_xero_tokens(
        tenant_id=primary_tenant["tenantId"],
        org_name=primary_tenant["tenantName"],
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        expires_in=tokens.get("expires_in", 1800),
    )

    return {
        "status": "CONNECTED",
        "tenant_id": primary_tenant["tenantId"],
        "organization_name": primary_tenant["tenantName"],
    }



async def reconcile_xero_invoice(
    access_token: str,
    tenant_id: str,
    raw_reference: str,
    amount_paid: float,
    bank_account_code: str = "090",
):
    """Matches an unstructured bank reference to an AUTHORISED Xero invoice and posts payment."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }

    async with httpx.AsyncClient() as client:
        inv_res = await client.get(
            "https://api.xero.com/api.xro/2.0/Invoices?Statuses=AUTHORISED",
            headers=headers,
        )
        invoices = inv_res.json().get("Invoices", [])

        cleaned_ref = raw_reference.upper()
        matched_invoice = next(
            (inv for inv in invoices if inv.get("InvoiceNumber", "").upper() in cleaned_ref),
            None,
        )

        if not matched_invoice:
            return {"match_status": "UNMATCHED", "reason": "No matching AUTHORISED Xero invoice"}

        payment_payload = {
            "Payments": [
                {
                    "Invoice": {"InvoiceID": matched_invoice["InvoiceID"]},
                    "Account": {"Code": bank_account_code},
                    "Date": "2026-11-25",
                    "Amount": amount_paid,
                }
            ]
        }
        pay_res = await client.post(
            "https://api.xero.com/api.xro/2.0/Payments",
            headers=headers,
            json=payment_payload,
        )
        return {
            "match_status": "RECONCILED",
            "invoice_number": matched_invoice["InvoiceNumber"],
            "xero_status": pay_res.status_code,
        }
