# rag_agent.py (The Correct, Refactored Version)

from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_ollama import ChatOllama
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings

# --- Database Configuration ---
DB_USER = 'postgres'
DB_PASSWORD = '123654'  # Your password
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'argo_db'
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_rag_agent():
    """
    Initializes and returns the RAG components (retriever and agent).
    This function is imported by our Streamlit app.
    """
    print("🤖 Initializing RAG Agent for the UI...")

    # --- Initialize Components ---
    embedding_function = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
    vector_store = Chroma(persist_directory="./chroma_db", embedding_function=embedding_function)
    retriever = vector_store.as_retriever()
    print("   - Vector store loaded successfully.")

    db_engine = create_engine(DATABASE_URL)
    db = SQLDatabase(db_engine)
    llm = ChatOllama(model="llama3:8b", temperature=0)
    sql_agent_executor = create_sql_agent(llm, db=db, verbose=True) # Turn verbose on for debugging in terminal
    print("   - SQL agent created.")
    
    return retriever, sql_agent_executor

def run_rag_query(user_question, retriever, sql_agent_executor):
    """
    Takes a user question and the agent components and returns the AI's answer.
    """
    # 1. Retrieve context
    docs = retriever.invoke(user_question)
    context = "\n".join([d.page_content for d in docs])

    # 2. Build the prompt
    full_input = f"""
    Based on the question and the provided context, please answer the user's question.
    If the answer is in the context, you can answer directly.
    If the question requires data from the database, use your SQL tools.

    Context:
    ---
    {context}
    ---

    Question: {user_question}
    """

    # 3. Invoke the agent
    result = sql_agent_executor.invoke(
        {"input": full_input},
        {"handle_parsing_errors": True}
    )
    
    return result["output"]

# Note: There is no `if __name__ == "__main__"` block here.
# This file is now purely a library for app.py to use.