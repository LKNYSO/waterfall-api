import re
import uuid
from pinecone import Pinecone, ServerlessSpec
from sentence_transformers import SentenceTransformer

# =======================================================================
# 📡 100% FREE HARDENED VECTOR INITIALIZATION CORE
# =======================================================================
PINECONE_API_KEY = "pcsk_3eEVBB_QTWeWA2W1TzrEhH2Q5KXRQ474i1zPx2XJcAQ18stJ1Fx5dCXXvSHGVDnxgUXEMP"
INDEX_NAME = "waterfall-pitch-knowledgebase"

# Initialize Free Local Math Embeddings Engine (Downloads automatically for free)
print("⏳ Initializing local open-source embeddings model matrix...")
model = SentenceTransformer('all-MiniLM-L6-v2') 
pc = Pinecone(api_key=PINECONE_API_KEY)

def create_vector_index_if_not_exists():
    """Defensively provisions a 100% free serverless vector index cloud bucket."""
    active_indexes = [index.name for index in pc.list_indexes()]
    if INDEX_NAME not in active_indexes:
        print(f"⏳ Provisioning free serverless cloud vector store bucket: '{INDEX_NAME}'...")
        pc.create_index(
            name=INDEX_NAME,
            dimension=384,  # all-MiniLM-L6-v2 generates ultra-fast 384-dimension weights
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1")
        )
        print("✅ Free Vector index infrastructure initialized successfully!")
    else:
        print(f"📡 Utilizing pre-existing cloud vector index: '{INDEX_NAME}'")

def semantic_text_splitter(raw_text: str, chunk_size: int = 800, overlap: int = 150) -> list:
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
    """Generates free local vector weights and streams them to Pinecone."""
    create_vector_index_if_not_exists()
    index = pc.Index(INDEX_NAME)
    
    print("⏳ Parsing chat history documentation into semantic slices...")
    data_chunks = semantic_text_splitter(raw_conversation_history)
    print(f"📦 Generated {len(data_chunks)} isolated text segments.")
    
    upsert_batch = []
    for idx, text_chunk in enumerate(data_chunks):
        print(f"🧬 Compiling embedding vector weights locally for chunk [{idx + 1}/{len(data_chunks)}]...")
        
        # 🚀 Calculated locally on your machine for zero cost!
        vector_weights = model.encode(text_chunk).tolist()
        
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
