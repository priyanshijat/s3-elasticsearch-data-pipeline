import sys
import logging
import requests
import json
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Configuration - CORRECT INDEX NAME
ELASTICSEARCH_HOST = "http://20.197.20.106:9200"
ELASTICSEARCH_INDEX = "my_index"  # Your actual index name
DATABASE_NAME = "prototype-data"
TABLE_NAME = "new_partition_table"

# Conservative Settings
BATCH_SIZE = 50000
MAX_RETRIES = 3
REQUEST_TIMEOUT = 120

def setup_elasticsearch_for_bulk_operations():
    """Optimize Elasticsearch for bulk operations"""
    try:
        settings = {
            "index": {
                "number_of_replicas": 0,
                "refresh_interval": "30s"
            }
        }
        response = requests.put(
            f"{ELASTICSEARCH_HOST}/{ELASTICSEARCH_INDEX}/_settings",
            json=settings,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        if response.status_code == 200:
            logger.info("✅ Optimized Elasticsearch for bulk operations")
        else:
            logger.warning(f"⚠️ Could not optimize settings: {response.text}")
    except Exception as e:
        logger.warning(f"⚠️ Setup optimization failed: {e}")

def get_existing_months_from_elasticsearch():
    """Get existing months from Elasticsearch"""
    try:
        logger.info("🔍 Checking existing months in Elasticsearch...")
        existing_months = set()
        
        agg_query = {
            "size": 0,
            "aggs": {
                "unique_months": {
                    "terms": {
                        "field": "month.keyword",
                        "size": 20
                    }
                }
            }
        }
        
        response = requests.get(
            f"{ELASTICSEARCH_HOST}/{ELASTICSEARCH_INDEX}/_search",
            json=agg_query,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            buckets = data['aggregations']['unique_months']['buckets']
            
            for bucket in buckets:
                month = bucket['key']
                count = bucket['doc_count']
                existing_months.add(str(month))
                logger.info(f"  📊 {month}: {count:,} records")
            
            logger.info(f"✅ Found {len(existing_months)} months in Elasticsearch: {sorted(existing_months)}")
            return existing_months
        else:
            logger.warning(f"⚠️ Could not fetch existing months: {response.text}")
            return set()
            
    except Exception as e:
        logger.error(f"❌ Error checking existing months: {e}")
        return set()

def get_available_months_from_athena(glueContext):
    """Get all available months from Athena table"""
    try:
        logger.info("🔍 Checking available months in Athena table...")
        
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME,
            table_name=TABLE_NAME,
            transformation_ctx="aws_elastic_search"
        )
        
        df = dynamic_frame.toDF()
        month_counts = df.groupBy("month").count().collect()
        available_months = {}
        
        for row in month_counts:
            month = row['month']
            count = row['count']
            available_months[month] = count
            logger.info(f"  📊 {month}: {count:,} records")
        
        logger.info(f"✅ Found {len(available_months)} months in Athena: {sorted(available_months.keys())}")
        return available_months
        
    except Exception as e:
        logger.error(f"❌ Error getting available months: {e}")
        return {}

def safe_get(row, field, default_value=None):
    """Safely get field value from row"""
    try:
        value = row[field]
        return value if value is not None else default_value
    except:
        return default_value

def validate_and_clean_document(doc):
    """Validate and clean document before sending to Elasticsearch"""
    try:
        if not doc.get('id'):
            doc['id'] = 'unknown'
        if not doc.get('month'):
            doc['month'] = 'unknown'
        
        if 'temperature' in doc:
            try:
                doc['temperature'] = float(doc['temperature'])
            except:
                doc['temperature'] = 0.0
        
        if 'humidity' in doc:
            try:
                doc['humidity'] = float(doc['humidity'])
            except:
                doc['humidity'] = 0.0
        
        if 'city' in doc and doc['city'] is None:
            doc['city'] = ''
            
        return doc
    except Exception as e:
        logger.warning(f"⚠️ Document validation error: {e}")
        return doc

def row_to_document_safe(row, columns):
    """Convert row to document safely handling missing columns"""
    doc = {
        "id": str(safe_get(row, 'id', '')),
        "month": str(safe_get(row, 'month', ''))
    }
    
    # Add ALL fields that exist in the schema
    field_mappings = {
        'month_num': ('int', 0),
        'value': ('int', 0),
        'temperature': ('float', 0.0),
        'humidity': ('float', 0.0),
        'ts': ('int', 0),
        'city': ('str', ''),
        'date': ('str', ''),
        'date_timestamp_ns': ('str', ''),
        'date_timestamp_converted': ('str', '')
    }
    
    for field, (data_type, default) in field_mappings.items():
        if field in columns:
            try:
                if data_type == 'int':
                    doc[field] = int(safe_get(row, field, default))
                elif data_type == 'float':
                    doc[field] = float(safe_get(row, field, default))
                else:  # string
                    doc[field] = str(safe_get(row, field, default))
            except:
                doc[field] = default
    
    return validate_and_clean_document(doc)

def send_batch_to_elasticsearch(batch_docs, batch_number):
    """Send batch to Elasticsearch with comprehensive error handling"""
    for attempt in range(MAX_RETRIES):
        try:
            bulk_data = ""
            for doc in batch_docs:
                action = {"index": {"_index": ELASTICSEARCH_INDEX}}
                bulk_data += json.dumps(action) + "\n"
                bulk_data += json.dumps(doc) + "\n"
            
            data_size_mb = len(bulk_data) / (1024 * 1024)
            logger.info(f"📤 Sending batch {batch_number} ({len(batch_docs):,} records, ~{data_size_mb:.1f}MB)")
            
            response = requests.post(
                f"{ELASTICSEARCH_HOST}/_bulk",
                data=bulk_data,
                headers={"Content-Type": "application/json"},
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errors'):
                    failed_count = 0
                    error_samples = []
                    
                    for item in result['items']:
                        if item['index'].get('error'):
                            failed_count += 1
                            if len(error_samples) < 2:
                                error_info = {
                                    'type': item['index']['error'].get('type', 'unknown'),
                                    'reason': item['index']['error'].get('reason', 'unknown')[:200]
                                }
                                error_samples.append(error_info)
                    
                    success_count = len(batch_docs) - failed_count
                    
                    if failed_count > 0:
                        logger.warning(f"⚠️ Batch {batch_number}: {success_count:,} success, {failed_count:,} failed")
                        
                        if failed_count == len(batch_docs):
                            logger.error(f"❌ BATCH {batch_number} COMPLETE FAILURE")
                            logger.error(f"🔍 Errors: {error_samples}")
                            return False, 0
                        else:
                            logger.info(f"✅ Batch {batch_number} partial: {success_count:,} records")
                            return True, success_count
                    else:
                        logger.info(f"✅ Batch {batch_number} success: {len(batch_docs):,} records")
                        return True, len(batch_docs)
                else:
                    logger.info(f"✅ Batch {batch_number} success: {len(batch_docs):,} records")
                    return True, len(batch_docs)
            else:
                error_msg = response.text[:500] if response.text else "No response text"
                logger.warning(f"⚠️ Batch {batch_number} attempt {attempt+1} - HTTP {response.status_code}: {error_msg}")
                
        except Exception as e:
            logger.warning(f"⚠️ Batch {batch_number} attempt {attempt+1} - Error: {str(e)}")
        
        if attempt < MAX_RETRIES - 1:
            wait_time = (attempt + 1) * 20
            logger.info(f"🔄 Retrying batch {batch_number} in {wait_time} seconds...")
            import time
            time.sleep(wait_time)
    
    logger.error(f"❌ Batch {batch_number} failed after {MAX_RETRIES} attempts")
    return False, 0

def main():
    try:
        logger.info("🚀 Starting SMART data transfer...")
        
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        
        logger.info("✅ Spark session initialized")
        
        # Setup Elasticsearch for bulk operations
        setup_elasticsearch_for_bulk_operations()
        
        # Get existing months from Elasticsearch
        existing_months_es = get_existing_months_from_elasticsearch()
        
        # Get available months from Athena
        available_months_athena = get_available_months_from_athena(glueContext)
        
        if not available_months_athena:
            logger.error("❌ No data found in Athena table!")
            return
        
        # Identify months to transfer
        months_to_transfer = []
        for month in available_months_athena.keys():
            if month not in existing_months_es:
                months_to_transfer.append(month)
        
        if not months_to_transfer:
            logger.info("🎉 All months already exist in Elasticsearch! No transfer needed.")
            return
        
        logger.info(f"📊 Months to transfer (not in Elasticsearch): {sorted(months_to_transfer)}")
        
        # Process each month
        total_transferred_all_months = 0
        
        for month in months_to_transfer:
            try:
                logger.info(f"🔄 Processing month: {month}")
                
                # Read data for this month
                dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
                    database=DATABASE_NAME,
                    table_name=TABLE_NAME,
                    transformation_ctx=f"aws_elastic_search_{month}",
                    push_down_predicate=f"month='{month}'"
                )
                
                df = dynamic_frame.toDF()
                month_count = df.count()
                logger.info(f"📊 {month} records found: {month_count:,}")
                
                if month_count == 0:
                    logger.warning(f"⚠️ No records found for {month}, skipping...")
                    continue
                
                # Get schema information
                columns = df.columns
                logger.info(f"📋 Columns available for {month}: {columns}")
                
                # Convert to documents
                documents_rdd = df.rdd.map(lambda row: row_to_document_safe(row, columns))
                new_records_count = documents_rdd.count()
                logger.info(f"📊 New {month} records to transfer: {new_records_count:,}")
                
                if new_records_count == 0:
                    logger.info(f"✅ All {month} records already exist in Elasticsearch")
                    continue
                
                # Calculate batches
                total_batches = (new_records_count + BATCH_SIZE - 1) // BATCH_SIZE
                logger.info(f"📦 Total batches for {month}: {total_batches}")
                
                # Process batches
                total_transferred_month = 0
                
                for batch_num in range(total_batches):
                    start_idx = batch_num * BATCH_SIZE
                    end_idx = min((batch_num + 1) * BATCH_SIZE, new_records_count)
                    
                    batch_docs = documents_rdd.zipWithIndex() \
                        .filter(lambda x: start_idx <= x[1] < end_idx) \
                        .map(lambda x: x[0]) \
                        .collect()
                    
                    if not batch_docs:
                        break
                        
                    logger.info(f"📦 Processing {month} batch {batch_num+1}/{total_batches} ({len(batch_docs):,} records)")
                    
                    success, transferred_count = send_batch_to_elasticsearch(batch_docs, batch_num+1)
                    
                    if success:
                        total_transferred_month += transferred_count
                        total_transferred_all_months += transferred_count
                        progress = (total_transferred_month / new_records_count) * 100
                        logger.info(f"📈 {month} progress: {total_transferred_month:,}/{new_records_count:,} ({progress:.1f}%)")
                    else:
                        logger.error(f"❌ {month} batch {batch_num+1} failed, skipping...")
                
                logger.info(f"✅ {month} transfer completed: {total_transferred_month:,} records")
                
            except Exception as e:
                logger.error(f"❌ Error processing {month}: {e}")
                continue
        
        # Final summary
        logger.info(f"🎉 SMART TRANSFER COMPLETED!")
        logger.info(f"📊 Total records transferred: {total_transferred_all_months:,}")
        logger.info(f"📊 Months transferred: {months_to_transfer}")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
