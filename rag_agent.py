# rag_agent.py

from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_ollama import ChatOllama
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings

# --- Database Configuration ---
DB_USER = 'postgres'
DB_PASSWORD = '123456'  # Your password
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'argo_db'
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def main():
    print("🤖 Initializing RAG Agent...")

    # --- Initialize Components ---
    embedding_function = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
    vector_store = Chroma(persist_directory="./chroma_db", embedding_function=embedding_function)
    retriever = vector_store.as_retriever()
    print("   - Vector store loaded successfully.")

    db_engine = create_engine(DATABASE_URL)
    db = SQLDatabase(db_engine)
    llm = ChatOllama(model="llama3:8b", temperature=0)
    sql_agent_executor = create_sql_agent(llm, db=db, verbose=True)
    print("   - SQL agent created.")

    print("\n✅ RAG agent is ready! Ask your questions.")
    print("   Type 'exit' to quit.")

    while True:
        try:
            user_question = input("\nYour question: ")
            if not user_question:
                continue
            if user_question.lower() == 'exit':
                break

            # --- RAG Steps ---
            # 1. Retrieve context
            docs = retriever.invoke(user_question) # Using .invoke() as recommended
            context = "\n".join([d.page_content for d in docs])

            # 2. Build the new, simpler prompt
            full_input = f"""
            Based on the question and the provided context, please answer the user's question.
            The context may contain helpful information about the database schema or definitions.
            If the answer is in the context, you can answer directly.
            If the question requires data from the database, use your SQL tools.

            Context:
            ---
            {context}
            ---

            Question: {user_question}
            """

            # 3. Invoke the agent with the new prompt and error handling
            result = sql_agent_executor.invoke(
                {"input": full_input},
                {"handle_parsing_errors": True} # <-- ADDED ERROR HANDLING
            )

            print("\nAI Response:")
            print(result["output"])

        except Exception as e:
            print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    main()