import os
import re
import time
import uuid
from pinecone import Pinecone, ServerlessSpec
import google.generativeai as genai
# =======================================================================
# 📡 100% FREE HARDENED VECTOR INITIALIZATION CORE
# =======================================================================
PINECONE_API_KEY = "pcsk_3eEVBB_QTWeWA2W1TzrEhH2Q5KXRQ474i1zPx2XJcAQ18stJ1Fx5dCXXvSHGVDnxgUXEMP"
GEMINI_KEY = "AQ.Ab8RN6L506wCnCCODgwov_JxUJY1E-7mNt1JG1hWdjWYvowrQA"
INDEX_NAME = "waterfall-pitch-knowledgebase"

# Initialize Cloud Clients
pc = Pinecone(api_key=PINECONE_API_KEY)
genai.configure(api_key=GEMINI_KEY)

def create_vector_index_if_not_exists():
    """Defensively provisions a 100% free serverless vector index cloud bucket."""
    active_indexes = [index.name for index in pc.list_indexes()]

    # 🚀 THE FIX: Force-delete the old 384 index to allow the clean 768 setup to build!
    if INDEX_NAME in active_indexes:
        print(f"🗑️ Purging obsolete legacy 384-dimension index: '{INDEX_NAME}'...")
        pc.delete_index(INDEX_NAME)
        print("✅ Obsolete index deleted successfully!")
        while INDEX_NAME in [idx.name for idx in pc.list_indexes()]:
            time.sleep(1)
    
        print(f"⏳ Provisioning free serverless cloud vector store bucket: '{INDEX_NAME}'...")
        pc.create_index(
            name=INDEX_NAME,
            dimension=768,  # Google text-embedding-004 generates high-resolution 768-dimension weights
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1")
        )

          # 🚀 THE CRITICAL TIME SYNC FIX: Poll the cloud index state status until it is completely ACTIVE!
    print("⏳ Index creation initialized. Waiting for cloud node activation routing...")
    while True:
        try:
            desc = pc.describe_index(INDEX_NAME)
            # Check if the status is active and fully provisioned
            if desc.status.get('ready') == True or desc.status.get('state') == 'Ready':
                break
        except Exception:
            # Catch transient 404s safely while the index is spawning up from scratch
            pass
        time.sleep(2)
        
    print("✅ High-resolution 768-dimension vector index initialized and 100% READY!")

def semantic_text_splitter(raw_text: str, chunk_size: int = 1200, overlap: int = 200) -> list:
    """Splits transcripts into overlapping segments to preserve technical context."""
    sanitized_text = re.sub(r'\n{3,}', '\n\n', raw_text)
    chunks = []
    start = 0
    text_length = len(sanitized_text)
    while start < text_length:
        end = min(start + chunk_size, text_length)
        chunks.append(sanitized_text[start:end])
        start += (chunk_size - overlap)
    return chunks

def build_and_upload_knowledgebase(raw_conversation_history: str):
    """Generates free cloud vector weights and streams them to Pinecone."""
    create_vector_index_if_not_exists()
    index = pc.Index(INDEX_NAME)
    
    print("⏳ Parsing chat history documentation into semantic slices...")
    data_chunks = semantic_text_splitter(raw_conversation_history)
    print(f"📦 Generated {len(data_chunks)} text segments.")
    
    upsert_batch = []
    for idx, text_chunk in enumerate(data_chunks):
        print(f"🧬 Compiling embedding vector weights via Google Cloud [{idx + 1}/{len(data_chunks)}]...")
        
        # 🚀 THE FIX: Switch to the active gemini-embedding-001 model with custom 768 parameters
        response = genai.embed_content(
            model="models/gemini-embedding-001",
            content=text_chunk,
            task_type="retrieval_document",
            output_dimensionality=768
        )
        vector_weights = response['embedding']

        
        metadata = {
            "chunk_id": idx,
            "raw_text": text_chunk,
            "category": "technical_architecture_history"
        }
        upsert_batch.append((f"chunk_id_{uuid.uuid4().hex[:8]}", vector_weights, metadata))
    
    print(f"🚀 Streaming vector payloads over the web to your free Pinecone index...")
    index.upsert(vectors=upsert_batch)
    print("✨ SUCCESS! Your entire technical history is officially written to the vector core for free!")

if __name__ == "__main__":
    RAW_HISTORY_DATA_BLOCK = """
    PLATFORM ARCHITECTURE SUMMARY:
    - Server: FastAPI hosted on Render (IPv4 container network).
    - Database: PostgreSQL on Supabase (Frankfurt, Germany) connected via explicit IPv4 connection proxy pooler handle.
    - Security: Proxy-Aware Middleware IP Firewall whitelisting laptop public footprint: 41.126.203.134.
    - Reporting Engine: Compiles ReportLab PDFs and pushes them to Gmail via SendGrid Web API client SDK on port 443.

    BUSINESS, SUBSCRIPTION, & PRICING MODEL MATRIX:
    - Target Audience: B2B corporate banking desks and institutional clearing houses requiring low-latency ledger isolation wrappers.
    - Pricing Tier 1 Growth & Mid-Market (0 to 50,000 matches/month). Price: R1.50 per successful match. Target: Municipalities,rental agencies, emerging e-commerce platforms and regional subscription services.
    - Pricing Tier 2 High-Volume Enterprise (50,001 to 200,000 matches/month). Price: R0.95 per successful match. Target: Large-scale financial institutions and enterprise clients.
    - Pricing Tier 3 Ultra-High-Volume Telco/Utility (200,001 to 1,000,000+ matches/month). Price: R0.45 per successful match. Target: Telecommunications and utility companies with high transaction volumes.
    - Reasoning:udges, our pricing model is explicitly structured to capture ultra-high-volume B2B enterprise transactions through a Tiered Step-Down Per-Match Strategy. We do not charge for failed attempts, server uptimes, or API pings; we bill strictly for successful ledger state changes [B].
      For a low-volume merchant, a match costs R1.50. But for an enterprise anchor client like MTN processing half a million payments a month, our tier drops automatically to just R0.45 per match. Compare that to the alternative: a manual corporate accounting triage team costs an enterprise an average of R12.00 to R15.00 per manually checked bank statement line in human resource overhead. Waterfall API doesn't just accelerate cash flow; we shrink internal operational unit costs by over 95% while scaling our own high-margin infrastructure revenue streams seamlessly
    
    COMPETITIVE LANDSCAPE & MARKET POSITIONING:
    How the Platforms Stack Up Against a Waterfall API
    - 1. Xero: The General Ledger (Downstream Consumer)
        • The Relationship: Not a competitor; it is a recipient. 
        • The Landscape: Xero cannot compute complex fund distributions or multi-tiered creditor logic natively. Instead, a Waterfall API calculates the exact, granular payout allocations and then pushes those clean, pre-reconciled journal entries downstream into Xero to update your books.
        - Waterfall API vs. Xero: The Infrastructure Utility vs. The Bookkeeping App
        	The Core Conflict: Real-Time Developer API vs. Retrospective Human Accounting
	        • The Xero Model: Xero is an end-user accounting suite designed for human eyes. It relies on pulling daily batch bank statement files retrospectively (often with a 24-hour delay) and forcing small business owners or human accountants to create manual "bank rules" or click "OK" to match rows.
	        • The Waterfall Moat: Waterfall API is a developer-first real-time infrastructure middleware layer [B]. We do not wait 24 hours for a batch dump, and we do not provide a user interface for manual bookkeeping entries. Waterfall API connects directly to real-time bank webhooks, executing deterministic dual-track text matching inside short-lived server memory within milliseconds of the money clearing the bank rails [B].
	        • The Scale Moat: Xero's internal matching tables lock up and slow down when hit with high transaction volumes. A telecom enterprise like MTN processing 500,000 monthly transactions cannot run their business operational tracking on a general-ledger small business suite. Waterfall API handles massive scale natively through high-speed PostgreSQL partial indexing.
            • The Financial Moat: Xero charges a fixed, predictable subscription fee ($29/month) which is excellent for a local coffee shop but completely unaligned with enterprise value. Waterfall API charges strictly for successful ledger state changes via our Tiered Step-Down Per-Match model (R1.50 sliding down to R0.45), transforming an operational cost burden into a highly efficient, elastic utility
    - 2. Airwallex: The Global Payment Rail (Infrastructure Partner)
        • The Relationship: Complementary Infrastructure.
        • The Landscape: Airwallex provides the global payment rails for cross-border transactions. Waterfall API leverages these rails to execute complex fund distributions, ensuring that the right amounts reach the right parties in real-time.
        - Waterfall API vs. Tipalti: The Revenue AR Core vs. The Payout AP Engine
	        The Core Conflict: Inbound Accounts Receivable (AR) vs. Outbound Accounts Payable (AP)
	        • The Tipalti Model: Tipalti is an automation behemoth built almost exclusively for Accounts Payable (AP) and procurement loops. They automate mass payouts out of a business to global suppliers, contractors, and creators, using AI to perform 2-way and 3-way invoice matching against purchase orders generated internally.
	        • The Waterfall Moat: Tipalti optimizes for the cash outgoing pipeline. Waterfall API targets the massive, high-risk problem of Accounts Receivable (AR)—matching unpredictable, high-volume incoming customer bank deposits to open invoices [B]. Matching payouts is easy because the business controls the payment execution parameters; matching incoming revenue is exceptionally hard because the business has zero control over what a messy human customer types into their banking app reference field [B].
	        • The Integration Moat: Tipalti requires massive corporate ERP structural changes (like heavy NetSuite or Sage Intacct integration overhauls) that take months to configure and deploy. Waterfall API is lightweight and developer-first, deploying seamlessly via secure machine-to-machine OAuth 2.0 Client Credentials webhooks that hook into any custom programming stack in days [B].
            • The Financial Moat: Tipalti charges a heavy monthly software access fee starting at €99/month, which scales up drastically based on custom enterprise module scoping. Waterfall API aligns purely with client growth metrics: if you don't process a successful match, you pay nothing 
    - 3. Tipalti: The Payment Orchestration Layer (Adjacent Competitor)
        • The Relationship: Adjacent Competitor.
        • The Landscape: Tipalti offers payment orchestration capabilities, but Waterfall API provides a more comprehensive solution for complex fund distributions and real-time payout allocations.
        -  Waterfall API vs. Tipalti: The Revenue AR Core vs. The Payout AP Engine
	       The Core Conflict: Inbound Accounts Receivable (AR) vs. Outbound Accounts Payable (AP)
	        • The Tipalti Model: Tipalti is an automation behemoth built almost exclusively for Accounts Payable (AP) and procurement loops. They automate mass payouts out of a business to global suppliers, contractors, and creators, using AI to perform 2-way and 3-way invoice matching against purchase orders generated internally.
	        • The Waterfall Moat: Tipalti optimizes for the cash outgoing pipeline. Waterfall API targets the massive, high-risk problem of Accounts Receivable (AR)—matching unpredictable, high-volume incoming customer bank deposits to open invoices [B]. Matching payouts is easy because the business controls the payment execution parameters; matching incoming revenue is exceptionally hard because the business has zero control over what a messy human customer types into their banking app reference field [B].
	        • The Integration Moat: Tipalti requires massive corporate ERP structural changes (like heavy NetSuite or Sage Intacct integration overhauls) that take months to configure and deploy. Waterfall API is lightweight and developer-first, deploying seamlessly via secure machine-to-machine OAuth 2.0 Client Credentials webhooks that hook into any custom programming stack in days [B].
            • The Financial Moat: Tipalti charges a heavy monthly software access fee starting at €99/month, which scales up drastically based on custom enterprise module scoping. Waterfall API aligns purely with client growth metrics: if you don't process a successful match, you pay nothing [B].
    - 4. HighRadius: Enterprise Cash Application
        • The Relationship: Adjacent Competitor.
        • The Landscape: HighRadius is an AI-powered matching engine built for enterprise ERPs. It is highly effective at scanning millions of open invoices to find an unmatched payment. However, its core design focuses on Accounts Receivable (AR) reconciliation rather than enforcing programmatic, conditional priority rules on outgoing funds
        - Waterfall API vs. HighRadius: Lightweight Cloud Middleware vs. Legacy ERP Heavyweight
	        The Core Conflict: Lightning-Fast Agile Deployment vs. Multi-Million-Rand ERP Monoliths
	        • The HighRadius Model: HighRadius is a giant enterprise software provider that uses AI matching models built strictly to plug deep inside massive corporate ERP frameworks (like Oracle, SAP, or Microsoft Dynamics).
	        • The Waterfall Moat: HighRadius requires massive corporate transformation budgets, dedicated software consulting teams, and 6 to 12 months of implementation overhead just to stand up. Waterfall API is an agile, lightweight cloud-native middleware layer [B]. By utilizing clear Pydantic data schemas and high-speed asynchronous processing workers, a single developer can mount Waterfall API into an active project workspace folder inside a single afternoon.
	        • The Data Moat (POPIA Compliance): HighRadius stores extensive historic consumer payment profiles and financial habits globally across cloud instances. Waterfall API is engineered around absolute POPIA Data Minimisation for the South African landscape [A]. We do not store clear-text names or raw account digits; everything is scrubbed inside short-lived, transient server memory and committed to disk strictly as secure 12-character SHA-256 hash footprints [A].
	        • The Financial Moat: HighRadius requires locked annual software licensing contracts worth millions of Rands upfront, completely shutting out growing mid-market companies. Waterfall API Tiered Step-Down Per-Match model allows an enterprise to start lean and naturally access our ultra-competitive R0.45 scaled tier as their transactional volume expands [B].
    - 5. Lunos: B2B Smart Matching
        • The Relationship: Niche Alternative.
        • The Landscape: Lunos targets business revenue collections with unmatched bank deposits. It handles standard invoice matching but is not designed for structured finance operations or contractual debt waterfalls.
        - On paper, Lunos sounds like a direct threat because they explicitly target "B2B businesses with messy, unmatched invoice payments." However, when you analyze how they solve the problem versus how Waterfall API solves it, you will see that Lunos has built an expensive, high-friction application, while Waterfall API is a lean, developer-first infrastructure utility.
        The Core Conflict: AI-Agent Scraping vs. Deterministic API Gateway
	        1. The Lunos Approach: The "AI Accountant" (High Friction). Lunos approaches unmatched payments as a communication and workflow problem. They deploy AI "agents" or workflows that connect to the business email inboxes, scrape incoming data, read uploaded PDF remittance advices, or prompt human users to upload proof of payments to guess which invoice matches which bank line.
	        • The Flaw for High Volume: This model introduces massive SECURITY and LATENCY friction. High-volume enterprises (like a telecom giant processing 500,000 billing rows a month) refuse to give a third-party startup broad API access to scrape their corporate email servers or read raw customer attachments due to extreme security risks. Furthermore, scraping unstructured PDFs is SLOW, RESOURCE-HEAVY, and prone to HALLUCCINATIONS.
	        2. The Waterfall API Approach: Pure Data Engineering (Zero Friction). Waterfall API treats unmatched payments as a pure data engineering problem. We don't read emails, we don't look at attachments, and we don't ask users to change their behavior.
    - SUMMARY: Judges, the global reconciliation landscape is heavily bifurcated, and that is exactly where our opportunity lies. General ledger apps like Xero are designed for manual human bookkeeping rules retrospectively. Monoliths like HighRadius are locked behind multi-million-rand, slow-moving corporate ERP integration barriers. Gateways like Airwallex require merchants to leave their existing banks and move money onto closed-loop wallet networks.
		Waterfall API is an account-agnostic, developer-first Infrastructure Middleware Utility [B]. We do not move physical money, we do not read private emails like Lunos, and we do not force users to change their behavior. We sit directly behind existing enterprise bank feeds, executing deterministic, real-time dual-track substring matching inside transient memory within milliseconds [B].
        If anything, we compliment the existing ecosystem of enterprise reconciliation tools. We are not a competitor to Xero, HighRadius, or Tipalti; we are a complementary infrastructure utility that makes their downstream jobs easier and more efficient [B]. We view Waterfall API as a piece to the puzzle, not a replacement for the entire puzzle.
        More importantly, our unit economics protect enterprise margins natively [B]. While competitors penalize business growth by taking percentage cuts or charging massive locked annual software license fees, Waterfall API bills strictly on a tiered, flat step-down fee per successful match, scaling down to just R0.45 for our high-volume anchor accounts [B]. We deliver a FASTER, SAFER, POPIA-HARDENED DATA AUTOMATED SOLUTION at a FRACTION of the COST, MAKING MANUAL ACCOUNTING DESKS OBSOLETE [A, B].             • Our system operates entirely as a machine-to-machine developer API gateway. The merchant's system pushes clean parameters (POST /verify) [B], and the bank pushes clean payload streams (POST /bank-feed) [B]. Our engine executes deterministic, high-speed fuzzy substring matching entirely in transient memory within millisecond
    
    WATERFALL API PRESENTATION INFO:
    - Good evening, panel. Today, corporate businesses move trillions of Rands across the web daily. But under the hood, standard corporate bank servers are fragile. When thousands of customers pull statements, query data, or run audit requests at the same time, the main database slows to a crawl or crashes. This leads to costly server downtime, frozen apps, and angry corporate clients.
      My name is Lukanyiso Brukwe, and I built Waterfall API—a hyper-stable, ultra-secure digital shield that stands between high-traffic internet demands and core corporate financial ledgers.
      Waterfall API acts as a powerful middleware filter. When an external system requests a heavy daily financial summary statement, Waterfall doesn't slow down the main server. Instead, it instantly responds in under 300 milliseconds to acknowledge the request.
      Then, entirely in the background, our system quietly connects to our remote cloud databases, assembles a beautifully formatted high-resolution financial statement PDF, and securely drops it directly into the client's inbox using automated web delivery rails. The user gets a lightning-fast experience, and the core banking database remains perfectly safe and unburdened.
      For corporate financial systems, security is non-negotiable. Waterfall API features a built-in, zero-trust Network Smart Firewall. It automatically detects and reads traffic routing paths, instantly separating verified corporate partner streams from unauthorized external computer footprints. If an unrecognized source attempts to tap into the ledger routes, our system flags it and cuts the connection immediately at the front gate before a single byte of financial data is exposed.
      Our market strategy is highly scalable, targeting B2B corporate banking desks and institutional clearing houses that require low-latency ledger isolation wrappers. We offer a tiered pricing model that rewards high-volume enterprise clients with lower costs per transaction, making our solution both efficient and cost-effective.
    """
    build_and_upload_knowledgebase(RAW_HISTORY_DATA_BLOCK)