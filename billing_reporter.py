import os
import sys
import smtplib
import psycopg2
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
import base64
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

try:
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
except ImportError:  # pragma: no cover - dependency is installed through requirements.txt
    letter = None
    SimpleDocTemplate = None
    Paragraph = None
    Spacer = None
    Table = None
    TableStyle = None
    getSampleStyleSheet = None
    ParagraphStyle = None
    colors = None

# =======================================================================
# 📡 INFRASTRUCTURE CREDENTIALS & CONSTANTS
# =======================================================================
DATABASE_HOST = "aws-0-eu-central-1.pooler.supabase.com"
DATABASE_PORT = "5432"
DATABASE_NAME = "postgres"
DATABASE_USER = "postgres.faonspoqastaempflpee"
DATABASE_PASSWORD = "mp3EjNdA9A3qFv0e"

# Transactional Mail Relay Carrier Parameters (SendGrid Example Relay)
SMTP_HOST = "smtp.sendgrid.net"
SMTP_PORT = "465"  # Enforces native SSL right out of the box
SMTP_USER = "apikey" # SendGrid requires the literal username 'apikey'
SMTP_PASSWORD = "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"
RECIPIENT_EMAIL = "blukhanyiso@gmail.com"
SENDER_EMAIL = "blukhanyiso@gmail.com"

def compile_daily_financial_ledger_pdf() -> str:
    """Queries Supabase, unpacks positional database tuples, and builds the statement PDF."""
    if SimpleDocTemplate is None or getSampleStyleSheet is None or colors is None:
        raise RuntimeError(
            "reportlab is required to generate PDF reports. Install it with 'pip install -r requirements.txt'."
        )

    today_str = datetime.now().strftime("%Y-%m-%d")
    output_filename = f"Waterfall_Daily_Financial_Report_{today_str}.pdf"
    
    print(f"⏳ Connecting straight to production database core on port 5432...")
    # 🚀 THE FIX: Separate your credentials explicitly into separate arguments 
    # to stop special characters like '@' or '%' from breaking the network connection string!
    conn = psycopg2.connect(
        host="aws-0-eu-central-1.pooler.supabase.com",
        port="5432",
        database="postgres",
        user="postgres.faonspoqastaempflpee",
        password="mp3EjNdA9A3qFv0e"
    )
    cursor = conn.cursor()
    
    # Query all records verified or created within the trailing 24-hour cycle
    query = """
        SELECT id, invoice_id, expected_reference, amount, status 
        FROM customer_payments 
        WHERE created_at >= NOW() - INTERVAL '1 day'
        ORDER BY created_at DESC;
    """
    cursor.execute(query)
    records = cursor.fetchall()
    
    print(f"📦 Successfully extracted {len(records)} ledger rows from the cloud instance.")
    
    # Initialize the ReportLab PDF Canvas Layout Document
    doc = SimpleDocTemplate(output_filename, pagesize=letter, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    story = []
    styles = getSampleStyleSheet()
    
    # Custom Brand Palette Typography Styles
    title_style = ParagraphStyle(
        'DocTitle', parent=styles['Heading1'], fontName='Helvetica-Bold', fontSize=22, 
        textColor=colors.HexColor('#0B0F19'), spaceAfter=6
    )
    meta_style = ParagraphStyle(
        'DocMeta', parent=styles['Normal'], fontName='Helvetica', fontSize=10, 
        textColor=colors.HexColor('#9CA3AF'), spaceAfter=20
    )
    
    # Document Header Elements
    story.append(Paragraph("Waterfall API: Financial Ledger Audit Statement", title_style))
    story.append(Paragraph(f"Generated Execution Cycle: {today_str} • System Status: Operational", meta_style))
    story.append(Spacer(1, 10))
    
    # Define Structured Report Table Columns Layout Matrix
    # We pass explicit Paragraph flowables inside the cells to prevent tracking truncation
    table_data = [[
        Paragraph("<b>Transaction ID</b>", styles['Normal']),
        Paragraph("<b>Invoice ID</b>", styles['Normal']),
        Paragraph("<b>Expected Ref</b>", styles['Normal']),
        Paragraph("<b>Amount</b>", styles['Normal']),
        Paragraph("<b>State</b>", styles['Normal'])
    ]]
    
    total_revenue = 0.0
    
    for row in records:
        # 🚀 ALIGNMENT SECURED: Extract variables using exact positional coordinates
        tx_id    = str(row[0])[:8] + "..." # Truncate UUID for scannable UI layout
        inv_id   = str(row[1])
        exp_ref  = str(row[2])
        amount   = float(row[3])
        status   = str(row[4])
        
        total_revenue += amount
        
        table_data.append([
            Paragraph(tx_id, styles['Normal']),
            Paragraph(inv_id, styles['Normal']),
            Paragraph(exp_ref, styles['Normal']),
            Paragraph(f"R {amount:,.2f}", styles['Normal']),
            Paragraph(status, styles['Normal'])
        ])
        
    # Append Total Aggregates Row onto the grid base lines
    table_data.append([
        Paragraph("<b>TOTAL ACCOUNT VALUE COMBINED</b>", styles['Normal']),
        "", "", Paragraph(f"<b>R {total_revenue:,.2f}</b>", styles['Normal']), ""
    ])
    
    # Inject Table Block Matrix Styles
    report_table = Table(table_data, colWidths=[90, 140, 110, 90, 100])
    report_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#151D30')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('BOTTOMPADDING', (0,0), (-1,0), 8),
        ('TOPPADDING', (0,0), (-1,0), 8),
        ('GRID', (0,0), (-1,-2), 0.5, colors.HexColor('#E5E7EB')),
        ('LINEBELOW', (0,-1), (-1,-1), 1.5, colors.HexColor('#0B0F19')),
        ('TOPPADDING', (0,-1), (-1,-1), 10),
    ]))
    
    story.append(report_table)
    doc.build(story)
    
    cursor.close()
    conn.close()
    print(f"✨ Report compiled cleanly onto temporary container volume disk storage: {output_filename}")
    return output_filename

def dispatch_pdf_report_via_smtp(sender_override: str = None, recipient_override: str = None):
    """Executes full operational cycle pipeline: generates PDF, encodes binary data, and dispatches via Web API SDK."""
    try:
        active_sender = sender_override or "blukhanyiso@gmail.com"
        active_recipient = recipient_override or "blukhanyiso@gmail.com"
        
        print(f"🚀 Initializing high-performance SendGrid Web API delivery chain...")
        pdf_filepath = compile_daily_financial_ledger_pdf()
        
        # 📊 1. Compile the Primary Mail Content Envelope
        message = Mail(
            from_email=active_sender,
            to_emails=active_recipient,
            subject=f"📊 Waterfall API - Daily Financial Ledger Summary Statement [{datetime.now().strftime('%Y-%m-%d')}]",
            plain_text_content="Greetings Operations Desk,\n\nPlease find attached the comprehensive automated 24-hour financial transaction audit ledger statement compiled natively from your cloud cluster database.\n\nSystem Core Automation Engine"
        )
        
        # 📂 2. Read and Convert the compiled PDF binary data into Base64 format string parameters
        with open(pdf_filepath, "rb") as f:
            encoded_pdf_bytes = base64.b64encode(f.read()).decode()
            
        # 🔗 3. Attach the encoded canvas file block straight onto the payload transport wrapper
        attached_file = Attachment(
            FileContent(encoded_pdf_bytes),
            FileName(pdf_filepath),
            FileType("application/pdf"),
            Disposition("attachment")
        )
        message.attachment = attached_file
        
        # 📡 4. Initialize the official Web client using your hardcoded Secret API key password token string
        # 🚀 REPLACE THIS PLACEHOLDER WITH YOUR REAL 'SG.' SECRET KEY STRING FOR INTENTIONAL CLEARANCE
        secret_api_key = "SG.postgresql://postgres:mp3EjNdA9A3qFv0e@db.faonspoqastaempflpee.supabase.co:5432/postgres"
        
        sg_client = SendGridAPIClient(secret_api_key)
        response = sg_client.send(message)
        
        print(f"✨ SendGrid Web API response transaction cleared! Status Code Badge: {response.status_code}")
        
        # Volatile Disk Housekeeping: Wipe the temporary local PDF trace to protect memory boundaries
        if os.path.exists(pdf_filepath):
            os.remove(pdf_filepath)
            
        print("🏆 SUCCESS! Financial statement distribution loop executed flawlessly across the HTTP REST gateway!")
        
    except Exception as e:
        print(f"🚨 [CRITICAL WEB API RUNTIME ERROR] Sequence collapsed: {str(e)}")
        raise e

if __name__ == "__main__":
    # This block executes natively when called by the automated 24-hour Render Cron Job worker container
    dispatch_pdf_report_via_smtp()
