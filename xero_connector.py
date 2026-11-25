import os
import base64
import httpx
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/v1/xero", tags=["Xero Accounting Bridge"])

XERO_CLIENT_ID = os.getenv("XERO_CLIENT_ID")
XERO_CLIENT_SECRET = os.getenv("XERO_CLIENT_SECRET")

# Automatically defaults to localhost:8000 for local testing unless overridden in Render
REDIRECT_URI = os.getenv(
    "XERO_REDIRECT_URI",
    "http://localhost:8000/api/v1/xero/callback"
)

SCOPES = "offline_access accounting.transactions accounting.contacts.read"


@router.get("/connect")
def initiate_xero_oauth():
    """Generates the self-serve OAuth 2.0 login URL matching the active environment."""
    if not XERO_CLIENT_ID:
        raise HTTPException(status_code=500, detail="XERO_CLIENT_ID is not configured.")

    auth_url = (
        "https://xero.com"
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
            "https://xero.com",
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
            "https://xero.com",
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        tenants = conn_res.json()

    if not tenants:
        raise HTTPException(status_code=400, detail="No Xero organizations were authorized.")

    primary_tenant = tenants[0]
    return {
        "status": "CONNECTED",
        "tenant_id": primary_tenant["tenantId"],
        "organization_name": primary_tenant["tenantName"],
        "access_token": tokens["access_token"],
        "refresh_token": tokens["refresh_token"],
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
            "https://xero.com",
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
            "https://xero.com",
            headers=headers,
            json=payment_payload,
        )
        return {
            "match_status": "RECONCILED",
            "invoice_number": matched_invoice["InvoiceNumber"],
            "xero_status": pay_res.status_code,
        }
