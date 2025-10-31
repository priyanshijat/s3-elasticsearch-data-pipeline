import json
import boto3
import requests
import pandas as pd
from io import StringIO
from time import sleep

# --- Configuration ---
S3_OUTPUT = "s3://sqhabshqesh/athena-results/"
DATABASE = "aws-elastic_search"
TABLE = "newpartitiontable"
ELASTICSEARCH_URL = "http://20.197.20.106:9200"
ELASTICSEARCH_INDEX = "new-prototype-data"
AWS_REGION = "eu-west-1"
BATCH_SIZE = 100000

# --- AWS Clients ---
athena = boto3.client("athena", region_name=AWS_REGION)
s3 = boto3.client("s3")

# --- Helper Functions ---
def run_athena_query(query):
    """Run Athena query and return DataFrame directly."""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": S3_OUTPUT},
    )
    query_id = response["QueryExecutionId"]

    # Wait for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        sleep(2)

    if status != "SUCCEEDED":
        raise Exception(f"Athena query failed with status: {status}")

    # Download result
    result_path = f"{S3_OUTPUT}{query_id}.csv"
    bucket_name = result_path.replace("s3://", "").split("/")[0]
    key = "/".join(result_path.replace("s3://", "").split("/")[1:])
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = obj["Body"].read().decode("utf-8")
    
    return pd.read_csv(StringIO(data))

def get_count_comparison():
    """Get count comparison for all months between Athena and Elasticsearch."""
    print("🔍 Getting count comparison for all months...")
    
    # Get counts from Athena
    query = f"""
        SELECT month, COUNT(*) as athena_count 
        FROM {TABLE} 
        GROUP BY month 
        ORDER BY month
    """
    athena_df = run_athena_query(query)
    athena_counts = dict(zip(athena_df['month'], athena_df['athena_count']))
    
    # Get counts from Elasticsearch
    es_query = {
        "size": 0,
        "aggs": {
            "months": {
                "terms": {
                    "field": "month",
                    "size": 20
                }
            }
        }
    }
    
    response = requests.get(
        f"{ELASTICSEARCH_URL}/{ELASTICSEARCH_INDEX}/_search",
        json=es_query,
        headers={"Content-Type": "application/json"},
        timeout=30
    )
    
    es_counts = {}
    if response.status_code == 200:
        data = response.json()
        for bucket in data['aggregations']['months']['buckets']:
            es_counts[bucket['key']] = bucket['doc_count']
    
    # Find months with count mismatch
    mismatch_months = []
    all_months = set(list(athena_counts.keys()) + list(es_counts.keys()))
    
    print("📊 COUNT COMPARISON:")
    print("Month".ljust(12) + "Athena".ljust(12) + "Elasticsearch".ljust(15) + "Status")
    print("-" * 50)
    
    for month in sorted(all_months):
        athena_count = athena_counts.get(month, 0)
        es_count = es_counts.get(month, 0)
        
        status = "✅ MATCH" if athena_count == es_count else "❌ MISMATCH"
        print(f"{month.ljust(12)}{str(athena_count).ljust(12)}{str(es_count).ljust(15)}{status}")
        
        if athena_count != es_count:
            mismatch_months.append(month)
    
    print(f"\n🎯 Mismatch months: {mismatch_months}")
    return mismatch_months

def get_es_existing_ids(month):
    """Get all existing IDs for a month from Elasticsearch."""
    print(f"🔍 Getting existing IDs for {month} from Elasticsearch...")
    
    all_ids = []
    search_after = None
    
    while True:
        query = {
            "size": 10000,
            "_source": ["id"],
            "query": {"term": {"month": month}},
            "sort": [{"id": "asc"}]
        }
        
        if search_after:
            query["search_after"] = search_after
            
        response = requests.get(
            f"{ELASTICSEARCH_URL}/{ELASTICSEARCH_INDEX}/_search",
            json=query,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ Error fetching IDs: {response.text}")
            break
            
        data = response.json()
        hits = data['hits']['hits']
        
        if not hits:
            break
            
        batch_ids = [hit['_source']['id'] for hit in hits]
        all_ids.extend(batch_ids)
        
        # Update search_after for pagination
        last_hit = hits[-1]
        search_after = last_hit['sort']
        
        if len(batch_ids) < 10000:  # Last batch
            break
    
    print(f"✅ Found {len(all_ids)} existing IDs for {month}")
    return set(all_ids)

def get_athena_all_ids(month):
    """Get all IDs for a month from Athena."""
    query = f"SELECT id FROM {TABLE} WHERE month = '{month}'"
    print(f"🔍 Getting all IDs for {month} from Athena...")
    
    df = run_athena_query(query)
    athena_ids = set(df['id'].tolist())
    
    print(f"✅ Found {len(athena_ids)} IDs in Athena for {month}")
    return athena_ids

def get_missing_data(month, missing_ids):
    """Get data for missing IDs from Athena."""
    if not missing_ids:
        return pd.DataFrame()
    
    # Process in chunks to avoid query size limits
    all_missing_data = []
    id_chunks = [list(missing_ids)[i:i + 10000] for i in range(0, len(missing_ids), 10000)]
    
    print(f"📥 Fetching {len(missing_ids)} missing records in {len(id_chunks)} chunks...")
    
    for i, id_chunk in enumerate(id_chunks):
        id_list = "', '".join(id_chunk)
        query = f"""
            SELECT id, month, month_num, value, temperature, humidity, ts 
            FROM {TABLE} 
            WHERE month = '{month}' AND id IN ('{id_list}')
        """
        
        print(f"🔄 Fetching chunk {i+1}/{len(id_chunks)} ({len(id_chunk)} records)")
        chunk_df = run_athena_query(query)
        all_missing_data.append(chunk_df)
    
    if all_missing_data:
        return pd.concat(all_missing_data, ignore_index=True)
    else:
        return pd.DataFrame()

def upload_to_elasticsearch(batch_docs, batch_num, total_batches):
    """Upload a batch of documents to Elasticsearch."""
    bulk_data = ""
    for doc in batch_docs:
        action = {"index": {"_index": ELASTICSEARCH_INDEX, "_id": doc["id"]}}
        bulk_data += json.dumps(action) + "\n"
        bulk_data += json.dumps(doc) + "\n"

    response = requests.post(
        f"{ELASTICSEARCH_URL}/_bulk",
        data=bulk_data.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=120
    )

    if response.status_code >= 300:
        print(f"❌ Batch {batch_num}/{total_batches} failed: {response.text}")
        return False
    else:
        result = response.json()
        if result.get('errors'):
            failed_count = sum(1 for item in result['items'] if item['index'].get('error'))
            if failed_count > 0:
                print(f"⚠️ Batch {batch_num}/{total_batches} had {failed_count} failed documents")
        print(f"✅ Batch {batch_num}/{total_batches} uploaded: {len(batch_docs):,} records")
        return True

def safe_get(row, key, default=""):
    return row[key] if key in row and pd.notna(row[key]) else default

def sync_mismatch_month(month):
    """Sync only mismatch month using ID-based comparison."""
    print(f"\n" + "="*60)
    print(f"🚀 Processing mismatch month: {month}")
    print("="*60)
    
    # 1️⃣ Get existing IDs from Elasticsearch
    es_ids = get_es_existing_ids(month)
    
    # 2️⃣ Get all IDs from Athena
    athena_ids = get_athena_all_ids(month)
    
    # 3️⃣ Find missing IDs
    missing_ids = athena_ids - es_ids
    print(f"📊 ID COMPARISON FOR {month}:")
    print(f"   Athena IDs: {len(athena_ids):,}")
    print(f"   Elasticsearch IDs: {len(es_ids):,}")
    print(f"   Missing IDs: {len(missing_ids):,}")
    
    if not missing_ids:
        print(f"✅ {month} IDs already match (count was wrong)")
        return 0
    
    # 4️⃣ Fetch missing data
    missing_df = get_missing_data(month, missing_ids)
    
    if missing_df.empty:
        print(f"❌ No data found for missing IDs")
        return 0
    
    print(f"📊 Missing records to transfer: {len(missing_df):,}")
    
    # 5️⃣ Prepare documents
    records = []
    for _, row in missing_df.iterrows():
        record = {
            "id": str(safe_get(row, "id", "")),
            "month": str(safe_get(row, "month", "")),
            "month_num": int(safe_get(row, "month_num", 0)),
            "value": int(safe_get(row, "value", 0)),
            "temperature": float(safe_get(row, "temperature", 0.0)),
            "humidity": float(safe_get(row, "humidity", 0.0)),
            "ts": int(safe_get(row, "ts", 0))
        }
        records.append(record)
    
    # 6️⃣ Upload in batches
    total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"📦 Starting transfer for {month}: {total_batches} batches")
    
    transferred_count = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        batch_num = i//BATCH_SIZE + 1
        print(f"📦 Processing {month} batch {batch_num}/{total_batches} ({len(batch):,} records)")
        
        success = upload_to_elasticsearch(batch, batch_num, total_batches)
        if success:
            transferred_count += len(batch)
        
        progress = (transferred_count / len(records)) * 100
        print(f"📈 {month} progress: {transferred_count:,}/{len(records):,} ({progress:.1f}%)")
    
    print(f"✅ Completed syncing {month}: {transferred_count:,} records transferred")
    return transferred_count

# --- Main Process ---
def main():
    try:
        print("🚀 Starting Smart ID-Based Sync (Count First, Then IDs)...")
        
        # 1️⃣ First get count comparison
        mismatch_months = get_count_comparison()
        
        if not mismatch_months:
            print("🎉 All months are already in sync!")
            return
        
        total_transferred = 0
        
        # 2️⃣ Process only mismatch months
        for month in mismatch_months:
            try:
                print(f"\n🎯 Processing mismatch month: {month}")
                transferred = sync_mismatch_month(month)
                total_transferred += transferred
            except Exception as e:
                print(f"❌ Error processing {month}: {e}")
                continue
        
        # Final summary
        print(f"\n" + "="*60)
        print(f"🎉 SMART SYNC COMPLETED!")
        print(f"📊 Total records transferred: {total_transferred:,}")
        print(f"📊 Mismatch months processed: {len(mismatch_months)}")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error in main process: {e}")
        import traceback
        print(traceback.format_exc())

# --- Run the script ---
if __name__ == "__main__":
    main()
