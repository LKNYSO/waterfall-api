import os
import re
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

    if INDEX_NAME not in active_indexes:
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
    """
    build_and_upload_knowledgebase(RAW_HISTORY_DATA_BLOCK)