import os
import re
import uuid
import openai
from pinecone import Pinecone, ServerlessSpec

# =======================================================================
# 📡 HARDENED VECTOR INITIALIZATION CORE
# =======================================================================
# 🚀 THE FIX: Hardcode your developer keys to avoid async worker context drops!
OPENAI_API_KEY = "your-actual-openai-api-key-here"
PINECONE_API_KEY = "your-actual-pinecone-api-key-here"
INDEX_NAME = "waterfall-pitch-knowledgebase"

# Initialize Cloud Clients
pc = Pinecone(api_key=PINECONE_API_KEY)
openai.api_key = OPENAI_API_KEY

def create_vector_index_if_not_exists():
    """Defensively provisions a low-latency serverless vector index cloud bucket."""
    active_indexes = [index.name for index in pc.list_indexes()]
    if INDEX_NAME not in active_indexes:
        print(f"⏳ Provisioning serverless cloud vector store bucket: '{INDEX_NAME}'...")
        pc.create_index(
            name=INDEX_NAME,
            dimension=1536,  # Matches text-embedding-3-small dimensionality
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1")
        )
        print("✅ Vector index cloud infrastructure initialized successfully!")
    else:
        print(f"📡 Utilizing pre-existing cloud vector index: '{INDEX_NAME}'")

def semantic_text_splitter(raw_text: str, chunk_size: int = 1000, overlap: int = 200) -> list:
    """Splits chat logs into overlapping fragments to preserve technical context."""
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
    """Generates dense semantic vector weights and streams them to Pinecone."""
    create_vector_index_if_not_exists()
    index = pc.Index(INDEX_NAME)
    
    print("⏳ Parsing chat history documentation into semantic slices...")
    data_chunks = semantic_text_splitter(raw_conversation_history)
    print(f"📦 Generated {len(data_chunks)} isolated text segments.")
    
    upsert_batch = []
    
    for idx, text_chunk in enumerate(data_chunks):
        print(f"🧬 Compiling embedding vector weights for chunk [{idx + 1}/{len(data_chunks)}]...")
        
        embedding_response = openai.Embedding.create(
            input=text_chunk,
            model="text-embedding-3-small"
        )
        vector_weights = embedding_response['data']['embedding']
        
        metadata = {
            "chunk_id": idx,
            "raw_text": text_chunk,
            "category": "technical_architecture_history"
        }
        
        upsert_batch.append((f"chunk_id_{uuid.uuid4().hex[:8]}", vector_weights, metadata))
    
    print(f"🚀 Streaming vector payloads over the web to your cloud cluster index...")
    index.upsert(vectors=upsert_batch)
    print("✨ SUCCESS! Your entire technical history is officially written to the vector core!")

if __name__ == "__main__":
    # Paste your raw text logs right inside this multi-line string block
    RAW_HISTORY_DATA_BLOCK = """
    PLATFORM ARCHITECTURE SUMMARY:
    - Server: FastAPI hosted on Render (IPv4 container network).
    - Database: PostgreSQL on Supabase (Frankfurt, Germany) connected via explicit IPv4 connection proxy pooler handle.
    - Security: Proxy-Aware Middleware IP Firewall whitelisting laptop public footprint: 41.126.203.134.
    - Reporting Engine: Compiles ReportLab PDFs and pushes them to Gmail via SendGrid Web API client SDK on port 443.
    """
    build_and_upload_knowledgebase(RAW_HISTORY_DATA_BLOCK)
