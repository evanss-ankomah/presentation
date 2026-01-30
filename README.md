# Code Review Presentation Guide
## Real-Time Data Ingestion Using Spark Streaming

This guide walks through the entire codebase in logical presentation order, explaining each file's purpose and key code sections.

---

## Presentation Order

1. **Infrastructure Layer** (Docker setup)
2. **Database Layer** (PostgreSQL schema)
3. **Data Source** (Event generator)
4. **Processing Layer** (Spark streaming)
5. **Live Demo**

---

# Part 1: Infrastructure Layer

## 1.1 Dockerfile

**Purpose**: Creates a custom Spark image with all dependencies pre-installed.

```dockerfile
FROM apache/spark:3.5.0-python3
```
- **Base Image**: Official Apache Spark 3.5 with Python 3
- **Why**: Provides Spark runtime + Python support out of the box

```dockerfile
USER root
RUN pip install --no-cache-dir faker==24.0.0 psycopg2-binary==2.9.9
```
- **faker**: Generates realistic fake data (names, UUIDs, etc.)
- **psycopg2-binary**: Python PostgreSQL adapter for connection testing

```dockerfile
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-42.7.1.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
```
- **JDBC Driver**: Downloads PostgreSQL JDBC driver into Spark's JAR directory
- **Why**: Required for Spark to write data to PostgreSQL via JDBC

```dockerfile
RUN mkdir -p /app/src /app/data/raw /app/data/processed /app/data/error /app/checkpoints /app/config
```
- **Directory Structure**:
  - `data/raw/` - CSV files land here (input for Spark)
  - `data/error/` - Invalid records quarantine
  - `checkpoints/` - Spark streaming state for fault tolerance

```dockerfile
CMD ["tail", "-f", "/dev/null"]
```
- **Keep Alive**: Container stays running for interactive commands

---

## 1.2 docker-compose.yml

**Purpose**: Orchestrates multi-container deployment (PostgreSQL + Spark).

### PostgreSQL Service
```yaml
postgres:
  image: postgres:15
  environment:
    POSTGRES_USER: postgres
    POSTGRES_PASSWORD: Amalitech.org
    POSTGRES_DB: ecommerce_events
```
- **Creates database** `ecommerce_events` on startup
- **Credentials** hardcoded for demo (use secrets in production)

```yaml
volumes:
  - postgres_data:/var/lib/postgresql/data
  - ./sql/postgres_setup.sql:/docker-entrypoint-initdb.d/init.sql
```
- **Persistent storage**: Named volume survives container restarts
- **Auto-initialization**: `init.sql` runs on first startup to create tables

```yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U postgres -d ecommerce_events"]
  interval: 10s
```
- **Health check**: Verifies database is ready before Spark starts

### Spark Service
```yaml
spark:
  build:
    context: .
    dockerfile: Dockerfile
  depends_on:
    postgres:
      condition: service_healthy
```
- **Builds from Dockerfile**: Custom image with dependencies
- **Dependency**: Waits for PostgreSQL health check to pass

```yaml
volumes:
  - ./src:/app/src
  - ./data:/app/data
  - ./checkpoints:/app/checkpoints
  - ./logs:/app/logs
```
- **Bind mounts**: Local files synced into container
- **Development workflow**: Edit locally, changes reflected immediately

```yaml
networks:
  - pipeline_network
```
- **Docker network**: Containers communicate via service names (`postgres`, `spark`)

---

# Part 2: Database Layer

## 2.1 postgres_setup.sql

**Purpose**: Defines the database schema for storing processed events.

### Main Table
```sql
CREATE TABLE IF NOT EXISTS user_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50) UNIQUE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    event_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Key Design Decisions**:
| Column | Purpose |
|--------|---------|
| `id SERIAL` | Auto-incrementing surrogate key |
| `event_id UNIQUE` | **Deduplication key** - prevents duplicate events |
| `CHECK constraint` | Enforces valid event types at DB level |
| `created_at DEFAULT` | Automatic ingestion timestamp |

### Performance Indexes
```sql
CREATE INDEX idx_user_events_user_id ON user_events(user_id);
CREATE INDEX idx_user_events_event_type ON user_events(event_type);
CREATE INDEX idx_user_events_timestamp ON user_events(event_timestamp);
CREATE INDEX idx_user_events_type_timestamp ON user_events(event_type, event_timestamp);
```

**Why indexes?**
- `user_id` - User activity queries
- `event_type` - Filter by action type
- `event_timestamp` - Time-range queries
- `(event_type, event_timestamp)` - Composite for common analytics

### Reporting Views
```sql
CREATE OR REPLACE VIEW v_event_summary AS
SELECT 
    event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS unique_users,
    ROUND(AVG(price)::numeric, 2) AS avg_price,
    ROUND(SUM(price)::numeric, 2) AS total_value
FROM user_events
GROUP BY event_type;
```

**Available Views**:
| View | Purpose |
|------|---------|
| `v_event_summary` | Stats by event type |
| `v_hourly_events` | Time-series for dashboards |
| `v_purchase_funnel` | User journey analysis |
| `v_top_products` | Best sellers by revenue |
| `v_recent_activity` | Last 100 events |

---

# Part 3: Data Source

## 3.1 data_generator.py

**Purpose**: Simulates real-time e-commerce user activity by generating fake events.

### Configuration
```python
EVENT_TYPES = {
    'view': 0.6,        # 60% of events
    'add_to_cart': 0.3, # 30% of events
    'purchase': 0.1     # 10% of events
}
```
- **Probability distribution**: Mimics real e-commerce funnel
- **Realistic**: Most users browse, fewer add to cart, fewest purchase

### Product Catalog
```python
CATEGORIES = {
    'Electronics': [
        ('Wireless Headphones', 79.99, 149.99),
        ('Smartphone Case', 15.99, 45.99),
        ...
    ],
    'Clothing': [...],
    ...
}
```
- **Tuple format**: `(product_name, min_price, max_price)`
- **Price randomization**: `random.uniform(min_price, max_price)`

### Event Generation
```python
def generate_event() -> Dict:
    category, product_name, price = select_product()
    return {
        'event_id': str(uuid.uuid4()),
        'user_id': generate_user_id(),
        'product_id': generate_product_id(),
        'product_name': product_name,
        'category': category,
        'price': price,
        'event_type': select_event_type(),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
```
- **UUID**: Guarantees unique event_id for deduplication
- **Timestamp**: Captured at generation time

### Rate Limiting
```python
class RateLimiter:
    def __init__(self, events_per_second: int):
        self.events_per_second = events_per_second
        self.interval = 1.0 / events_per_second
        
    def wait(self):
        if self.events_this_second >= self.events_per_second:
            sleep_time = 1.0 - (current_time - self.second_start)
            time.sleep(sleep_time)
```
- **Precise control**: Ensures exactly N events/second
- **Throttling**: Sleeps when rate exceeded

### CSV Output
```python
def write_events_to_csv(events: List[Dict], output_dir: str) -> str:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    filename = f"events_{timestamp}.csv"
    with open(filepath, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)
```
- **Unique filename**: Prevents overwrites using microseconds
- **CSV format**: Standard format Spark can read

### Graceful Shutdown
```python
def signal_handler(signum, frame):
    global shutdown_requested
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
```
- **Ctrl+C handling**: Finishes current batch before exiting
- **Clean shutdown**: No data corruption

---

# Part 4: Processing Layer

## 4.1 spark_streaming_to_postgres.py

**Purpose**: Core streaming pipeline - reads CSVs, transforms data, writes to PostgreSQL.

### Configuration Class
```python
class Config:
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
    JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    TRIGGER_INTERVAL = "10 seconds"
    MAX_FILES_PER_TRIGGER = 10
    MAX_RETRIES = 3
```
- **Environment variables**: Flexible deployment
- **Docker service name**: `postgres` resolves inside Docker network

### Schema Definition
```python
def get_event_schema() -> StructType:
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
```
- **Explicit schema**: Avoids runtime inference overhead
- **Best practice**: Always define schema for streaming

### Spark Session
```python
spark = (SparkSession.builder
    .appName("EcommerceEventStreaming")
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
```
- **JDBC JAR**: PostgreSQL driver location
- **Adaptive Query Execution**: Optimizes query plans at runtime
- **Shuffle partitions**: Reduced for small data volumes

### Data Validation
```python
def clean_and_validate_data(df: DataFrame) -> tuple:
    # Trim whitespace
    for col_name in ['event_id', 'user_id', ...]:
        cleaned_df = cleaned_df.withColumn(col_name, trim(col(col_name)))
    
    # Validate records
    validated_df = cleaned_df.withColumn(
        'is_valid',
        (
            col('event_id').isNotNull() &
            col('event_type').isin(Config.VALID_EVENT_TYPES) &
            (col('price') > 0) &
            col('event_timestamp').isNotNull()
        )
    )
    
    valid_df = validated_df.filter(col('is_valid') == True)
    invalid_df = validated_df.filter(col('is_valid') == False)
    return valid_df, invalid_df
```
- **Data quality**: Rejects malformed records
- **Routing**: Invalid records go to `data/error/`

### Deduplication
```python
def deduplicate_events(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(['event_id'])
```
- **In-batch dedup**: Removes duplicates within same micro-batch
- **Cross-batch dedup**: Handled by database UNIQUE constraint

### PostgreSQL Write with Retry
```python
def write_to_postgres_with_retry(batch_df: DataFrame, batch_id: int):
    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            final_df.write \
                .format("jdbc") \
                .option("url", Config.JDBC_URL) \
                .option("dbtable", "user_events") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            return  # Success
        except Exception as e:
            if "duplicate key" in str(e).lower():
                return  # Expected, not an error
            if attempt < Config.MAX_RETRIES:
                time.sleep(Config.RETRY_DELAY_SECONDS)
            else:
                raise
```
- **Retry logic**: Handles transient failures
- **Duplicate handling**: Gracefully skips known duplicates

### Streaming Query
```python
query = (streaming_df
    .writeStream
    .foreachBatch(write_to_postgres_with_retry)
    .outputMode("append")
    .trigger(processingTime=Config.TRIGGER_INTERVAL)
    .option("checkpointLocation", Config.CHECKPOINT_PATH)
    .start()
)
```
- **foreachBatch**: Custom sink logic per micro-batch
- **Checkpointing**: Stores offset/state for fault recovery
- **Trigger**: Processes every 10 seconds

---

# Part 5: Live Demo Script

## Demo Steps

### 1. Start Infrastructure
```bash
docker compose up -d --build
```

### 2. Start Streaming Job
```bash
docker compose exec spark spark-submit //app/src/spark_streaming_to_postgres.py
```

### 3. Generate Events (new terminal)
```bash
docker compose exec spark python3 //app/src/data_generator.py --events-per-second 10 --duration 30
```

### 4. Query Database
```bash
docker compose exec postgres psql -U postgres -d ecommerce_events -c "SELECT COUNT(*) FROM user_events;"
```

### 5. Show Spark UI
- Open http://localhost:4040
- Show Jobs tab, Streaming tab

### 6. Query Reporting Views
```sql
SELECT * FROM v_event_summary;
SELECT * FROM v_top_products LIMIT 5;
```

### 7. Show Log Files
```bash
cat logs/streaming_metrics.log | tail -20
```

---

# Key Talking Points

## Architecture Decisions

1. **Why Spark Structured Streaming?**
   - Exactly-once semantics via checkpointing
   - Unified batch/streaming API
   - Built-in fault tolerance

2. **Why foreachBatch sink?**
   - Full control over write logic
   - Custom retry/error handling
   - Per-batch metrics logging

3. **Why UNIQUE constraint + deduplication?**
   - Defense in depth
   - Handles both in-batch and cross-batch duplicates

4. **Why file-based source (not Kafka)?**
   - Simpler for demo purposes
   - Same principles apply to Kafka

## Performance Metrics

| Metric | Value |
|--------|-------|
| Throughput | 10-50+ events/sec |
| End-to-end latency | ~2-3 seconds |
| Write time per batch | 0.5-0.7 seconds |
| Validation overhead | ~0.6 seconds |

## Error Handling

- **Invalid records** → Routed to `data/error/`
- **DB connection failure** → Retry 3 times with 5s delay
- **Duplicate records** → Silently rejected by DB constraint
- **Crash recovery** → Resume from checkpoint

---

# Q&A Preparation

**Q: How does checkpointing work?**
A: Spark stores offset metadata in the checkpoint directory. On restart, it reads the last processed offset and resumes from there.

**Q: What happens if PostgreSQL is down?**
A: Retry logic attempts 3 times. If all fail, the batch fails and Spark will retry the entire batch on next trigger.

**Q: Can this scale to millions of events?**
A: Yes, with Kafka as source, multiple Spark executors, and PostgreSQL partitioning.

**Q: Why not use Spark's JDBC sink directly?**
A: We need custom logic for validation, error routing, and retry handling that the built-in sink doesn't provide.
