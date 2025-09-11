# create_vector_store.py

import chromadb
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import SentenceTransformerEmbeddings
from langchain_core.documents import Document  # <-- new import

def main():
    """
    Creates and persists a vector store with contextual information about the ARGO database.
    """
    print("🚀 Creating the vector store 'cheat sheet'...")

    # Your text data
    text_data = [
        "The 'floats' table contains metadata for each ARGO float, including a unique 'wmo_id'.",
        "The 'profiles' table stores information for each measurement cycle of a float, including the 'date', 'latitude', and 'longitude'.",
        "The 'measurements' table holds the scientific sensor readings like 'pressure', 'temperature', and 'salinity' for each profile.",
        "To link tables, the 'floats' table's 'float_id' connects to the 'profiles' table's 'float_id'.",
        "The 'profiles' table's 'profile_id' connects to the 'measurements' table's 'profile_id'.",
        "A common user question is to find the maximum or minimum value of a measurement, like temperature or salinity.",
        "Users often ask for data related to a specific float, identified by its 'wmo_id'. This requires joining the 'floats', 'profiles', and 'measurements' tables.",
        "Essential Ocean Variables (EOVs) available are temperature and salinity.",
        "WMO ID is the World Meteorological Organization identifier for a float platform.",
        "Pressure is measured in decibars and is related to depth.",
        "Salinity is measured in Practical Salinity Units (psu).",
        "Temperature is measured in degrees Celsius."
    ]

    # Convert strings to Document objects
    documents = [Document(page_content=text) for text in text_data]

    # Embedding model
    embedding_function = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")

    # Create the vector store
    vector_store = Chroma.from_documents(
        documents=documents,
        embedding=embedding_function,
        persist_directory="./chroma_db"
    )

    print(f"✅ Vector store created and saved to './chroma_db' with {len(documents)} documents.")
    print("This 'cheat sheet' is now ready for the AI agent to use.")

if __name__ == "__main__":
    main()