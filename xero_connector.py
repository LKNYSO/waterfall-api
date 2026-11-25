import os
import base64
import httpx
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/v1/xero", tags=["Xero Accounting Bridge"])

XERO_CLIENT_ID = os.getenv("XERO_CLIENT_ID")
XERO_CLIENT_SECRET = os.getenv("XERO_CLIENT_SECRET")
REDIRECT_URI = "https://onrender.com"

# Required OAuth 2.0 scopes to read open invoices and post reconciled payments
SCOPES = "offline_access accounting.transactions accounting.contacts.read"


@router.get("/connect")
def initiate_xero_oauth():
    """Generates the self-serve OAuth 2.0 login URL for accounting firms or SMEs."""
    auth_url = (
        "https://xero.com"
        f"?response_type=code&client_id={XERO_CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}&scope={SCOPES}"
    )
    return {"authorization_url": auth_url}


@router.get("/callback")
async def xero_oauth_callback(code: str):
    """Exchanges the authorization code for an access token and Xero Tenant ID."""
    creds = base64.b64encode(f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}".encode()).decode()

    async with httpx.AsyncClient() as client:
        # 1. Exchange code for OAuth tokens
        token_res = await client.post(
            "https://xero.com",
            headers={"Authorization": f"Basic {creds}", "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI},
        )
        if token_res.status_code != 200:
            raise HTTPException(status_code=400, detail="Xero token exchange failed")
        tokens = token_res.json()

        # 2. Fetch the connected Xero Organization (Tenant ID)
        conn_res = await client.get(
            "https://xero.com",
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        tenants = conn_res.json()

    # Store tokens['access_token'], tokens['refresh_token'], and tenants[0]['tenantId'] in Supabase
    return {
        "status": "CONNECTED",
        "tenant_id": tenants[0]["tenantId"],
        "organization_name": tenants[0]["tenantName"],
    }


async def reconcile_xero_invoice(
    access_token: str,
    tenant_id: str,
    raw_reference: str,
    amount_paid: float,
    bank_account_code: str = "090",  # Standard Xero Business Bank Account code
):
    """Matches an unstructured bank reference to an AUTHORISED Xero invoice and posts payment."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }

    async with httpx.AsyncClient() as client:
        # 1. Pull open (AUTHORISED) invoices from Xero
        inv_res = await client.get(
            "https://xero.com",
            headers=headers,
        )
        invoices = inv_res.json().get("Invoices", [])

        # 2. Sub-300ms in-memory reference isolation
        cleaned_ref = raw_reference.upper()
        matched_invoice = next(
            (inv for inv in invoices if inv.get("InvoiceNumber", "").upper() in cleaned_ref),
            None,
        )

        if not matched_invoice:
            return {"match_status": "UNMATCHED", "reason": "No matching AUTHORISED Xero invoice"}

        # 3. Post the payment allocation directly to Xero's General Ledger
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
            "https://xero.com",
            headers=headers,
            json=payment_payload,
        )
        return {
            "match_status": "RECONCILED",
            "invoice_number": matched_invoice["InvoiceNumber"],
            "xero_status": pay_res.status_code,
        }
