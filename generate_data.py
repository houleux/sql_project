from dotenv import load_dotenv
import os
import json
import time
import sqlite3
from google import genai
from google.genai import types

load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
DB_PATH = "saas_crm.db"
TARGET_ROWS = 500 
OUTPUT_FILE = "train_dataset.jsonl"
BATCH_SIZE = 10

client = genai.Client(api_key=API_KEY)

def get_schema_summary():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [r[0] for r in cursor.fetchall()]

    schema_str = ""
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [f"{r[1]} ({r[2]})" for r in cursor.fetchall()]
        schema_str += f"Table: {table}\nColumns: {', '.join(columns)}\n\n"
        
    conn.close()
    return schema_str

SCHEMA_CONTEXT = get_schema_summary()

def generate_batch(current_count):
    prompt = f"""
    You are an expert SQL Data Analyst. 
    I have a SQLite database with this schema:
    {SCHEMA_CONTEXT}

    Generate {BATCH_SIZE} unique pairs of "Natural Language Questions" and "SQL Queries".
    
    REQUIREMENTS:
    1. Diversity: Include filters (WHERE), aggregations (COUNT, SUM, AVG), and JOINS.
    2. Format: Return ONLY a raw JSON list of objects. No markdown.
    3. Structure: [ {{"question": "...", "sql": "..."}}, ... ]
    
    Generate challenging questions suitable for a business analyst.
    """

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema={
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "question": {"type": "STRING"},
                            "sql": {"type": "STRING"}
                        }
                    }
                }
            )
        )

        return json.loads(response.text)
    except Exception as e:
        print(f"Error generating batch: {e}")
        return []

def validate_and_format(pairs):
    valid_data = []
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    for pair in pairs:
        sql = pair.get("sql", "")
        question = pair.get("question", "")
        
        sql = sql.replace("```sql", "").replace("```", "").strip()
        
        try:
            cursor.execute(sql)

            jsonl_entry = {
                "instruction": "You are a text-to-SQL AI. Convert the question into a valid SQL query based on the schema.",
                "input": f"Question: {question}\nSchema: {SCHEMA_CONTEXT}",
                "output": sql
            }
            valid_data.append(jsonl_entry)
            print(f"✅ Valid: {question[:40]}...")
            
        except sqlite3.Error as e:
            print(f"❌ Rejected: {sql} | Error: {e}")
            
    conn.close()
    return valid_data


def main():
    if not SCHEMA_CONTEXT: return

    total_valid = 0
    
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r') as f:
            total_valid = sum(1 for _ in f)
        print(f"Resuming... found {total_valid} existing examples.")

    with open(OUTPUT_FILE, "a") as f:
        while total_valid < TARGET_ROWS:
            print(f"\n--- Generating batch (Current Total: {total_valid}/{TARGET_ROWS}) ---")
            
            raw_pairs = generate_batch(total_valid)
            if not raw_pairs: 
                time.sleep(2)
                continue
            
            valid_batch = validate_and_format(raw_pairs)
            
            for entry in valid_batch:
                f.write(json.dumps(entry) + "\n")
                
            total_valid += len(valid_batch)
            
            time.sleep(4) 

    print(f"\n🎉 DONE! Generated {total_valid} validated examples in {OUTPUT_FILE}")

if __name__ == "__main__":
    main()