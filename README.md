# Code Review Presentation Guide
## Real-Time Data Ingestion Using Spark Streaming

A detailed walkthrough explaining **what each piece of code does, why it was designed that way, and how it works**.

---

## Presentation Order

Present in this order to build understanding incrementally:
1. Infrastructure (Docker) → 2. Database (PostgreSQL) → 3. Data Source (Generator) → 4. Processing (Spark) → 5. Demo

---

# Part 1: Dockerfile

## What Is It?
A Dockerfile is a script that defines how to build a Docker image. It's like a recipe that lists all ingredients (dependencies) and steps to create a reproducible environment.

## Full Code with Explanations

```dockerfile
FROM apache/spark:3.5.0-python3
```

**WHAT**: Uses the official Apache Spark 3.5 image with Python 3 as our starting point.

**WHY**: 
- We don't want to install Spark from scratch (complex process)
- The official image is tested and optimized
- Python 3 variant includes PySpark bindings we need

**HOW**: Docker pulls this image from Docker Hub and uses it as the foundation for our custom image.

---

```dockerfile
USER root
RUN pip install --no-cache-dir faker==24.0.0 psycopg2-binary==2.9.9
```

**WHAT**: Switches to root user and installs two Python packages.

**WHY**:
- `faker`: Generates realistic fake data (names, UUIDs, addresses). We could use random strings, but `faker` creates more realistic test data.
- `psycopg2-binary`: Python library to connect to PostgreSQL. We use it to test if the database is ready before starting the streaming job.
- `--no-cache-dir`: Reduces image size by not caching downloaded packages.

**HOW**: The `RUN` command executes shell commands during image build. `pip install` downloads and installs the packages.

---

```dockerfile
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-42.7.1.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
```

**WHAT**: Downloads the PostgreSQL JDBC driver JAR file into Spark's JAR directory.

**WHY**: 
- Spark communicates with databases using JDBC (Java Database Connectivity)
- Spark doesn't include database drivers by default
- The JAR contains Java classes that know how to talk to PostgreSQL
- Placing it in `/opt/spark/jars/` ensures Spark automatically loads it

**HOW**: 
- `curl -L` downloads the file (follows redirects)
- `-o` specifies where to save it
- The URL points to the official PostgreSQL JDBC driver download

---

```dockerfile
RUN mkdir -p /app/src /app/data/raw /app/data/processed /app/data/error /app/checkpoints /app/config
```

**WHAT**: Creates the application directory structure.

**WHY each directory**:
| Directory | Purpose |
|-----------|---------|
| `/app/src` | Python source code |
| `/app/data/raw` | **Input**: CSV files land here, Spark watches this folder |
| `/app/data/processed` | Optional: could move completed files here |
| `/app/data/error` | **Quarantine**: Invalid/malformed records saved here for debugging |
| `/app/checkpoints` | **Critical**: Spark stores streaming state here for recovery |
| `/app/config` | Configuration files |

**HOW**: `mkdir -p` creates directories recursively (creates parent directories if they don't exist).

---

```dockerfile
WORKDIR /app
COPY src/ /app/src/
COPY config/ /app/config/
```

**WHAT**: Sets the working directory and copies local files into the image.

**WHY**: 
- `WORKDIR` means all subsequent commands run from `/app`
- `COPY` bakes our source code into the image so it's always available

**HOW**: During `docker build`, Docker copies files from your local machine into the image.

---

```dockerfile
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
```

**WHAT**: Sets environment variables.

**WHY**:
- `SPARK_HOME`: Tells applications where Spark is installed
- `PATH`: Adds Spark's bin folder so we can run `spark-submit` from anywhere
- `PYSPARK_PYTHON`: Tells Spark to use `python3` for PySpark execution

**HOW**: Environment variables are like global settings that any process inside the container can read.

---

```dockerfile
CMD ["tail", "-f", "/dev/null"]
```

**WHAT**: Default command when container starts - does nothing (keeps container alive).

**WHY**: 
- We want the container to stay running so we can `docker exec` into it
- We manually run commands like `spark-submit` via `docker exec`
- Without this, the container would start and immediately exit

**HOW**: `tail -f /dev/null` is a standard trick - it "follows" an empty file, which does nothing but never terminates.

---

# Part 2: docker-compose.yml

## What Is It?
Docker Compose orchestrates multiple containers. Instead of running complex `docker run` commands, we define everything in a YAML file.

## Full Code with Explanations

```yaml
services:
  postgres:
    image: postgres:15
```

**WHAT**: Defines a service called "postgres" using the official PostgreSQL 15 image.

**WHY**: PostgreSQL is our persistent storage. Version 15 is stable and supports all features we need.

**HOW**: Docker Compose pulls this image and creates a container from it.

---

```yaml
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: Amalitech.org
      POSTGRES_DB: ecommerce_events
```

**WHAT**: Passes environment variables to configure PostgreSQL.

**WHY**:
- `POSTGRES_USER/PASSWORD`: Authentication credentials
- `POSTGRES_DB`: Creates this database automatically on first startup. Without this, we'd have to manually create the database.

**HOW**: The PostgreSQL image reads these variables at startup and configures itself accordingly.

---

```yaml
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql/postgres_setup.sql:/docker-entrypoint-initdb.d/init.sql
```

**WHAT**: Mounts two volumes - one for persistent data, one for initialization.

**WHY**:
- `postgres_data`: **Persistence**. Without this, all data would be lost when the container restarts. This named volume stores database files on the host.
- `./sql/postgres_setup.sql:/docker-entrypoint-initdb.d/init.sql`: **Auto-initialization**. PostgreSQL automatically runs any `.sql` files in this special directory on first startup. Our table, indexes, and views are created automatically.

**HOW**: Docker creates a bridge between the container's filesystem and the host's filesystem. Changes in one appear in the other.

---

```yaml
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d ecommerce_events"]
      interval: 10s
      timeout: 5s
      retries: 5
```

**WHAT**: Defines a health check that verifies PostgreSQL is ready.

**WHY**:
- PostgreSQL takes several seconds to fully start
- Other services (Spark) need to wait for it
- `pg_isready` is PostgreSQL's built-in readiness checker
- Without this, Spark might try to connect before the database is ready

**HOW**: Docker runs this command every 10 seconds. If it succeeds, the container is "healthy". If it fails 5 times, it's "unhealthy".

---

```yaml
  spark:
    build:
      context: .
      dockerfile: Dockerfile
```

**WHAT**: Builds a custom image from our Dockerfile instead of pulling a pre-built image.

**WHY**: We need our custom image with the JDBC driver and Python dependencies installed.

**HOW**: `context: .` means Docker looks for the Dockerfile in the current directory. `dockerfile: Dockerfile` specifies which file to use.

---

```yaml
    depends_on:
      postgres:
        condition: service_healthy
```

**WHAT**: Spark only starts after PostgreSQL is healthy.

**WHY**: 
- Spark needs to write to PostgreSQL
- If Spark starts first, it would fail trying to connect
- The `service_healthy` condition uses the healthcheck we defined

**HOW**: Docker Compose manages the startup order based on dependencies and health status.

---

```yaml
    volumes:
      - ./src:/app/src
      - ./data:/app/data
      - ./checkpoints:/app/checkpoints
      - ./logs:/app/logs
```

**WHAT**: Maps local directories into the container.

**WHY**:
- `./src:/app/src`: **Development workflow**. Edit code locally, container sees changes immediately without rebuilding.
- `./data:/app/data`: **Data exchange**. CSV files generated are visible to Spark. Also allows inspecting data from host.
- `./checkpoints:/app/checkpoints`: **Fault tolerance**. Checkpoint data persists across container restarts.
- `./logs:/app/logs`: **Observability**. View log files from the host machine.

**HOW**: Bind mounts create a two-way sync between host and container directories.

---

```yaml
    networks:
      - pipeline_network

networks:
  pipeline_network:
    driver: bridge
```

**WHAT**: Creates and joins a custom Docker network.

**WHY**:
- Containers on the same network can communicate using service names
- The Spark container can connect to `postgres:5432` instead of hardcoding IP addresses
- Isolates our services from other containers on the system

**HOW**: Docker's bridge driver creates a virtual network. Containers get virtual IP addresses and DNS resolution for service names.

---

# Part 3: postgres_setup.sql

## What Is It?
SQL script that creates the database schema (table structure, indexes, views) when the PostgreSQL container first starts.

## Full Code with Explanations

```sql
CREATE TABLE IF NOT EXISTS user_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50) UNIQUE NOT NULL,
    ...
);
```

**WHAT**: Creates the main table if it doesn't already exist.

**WHY**:
- `IF NOT EXISTS`: Script can run multiple times without errors (idempotent)
- `SERIAL PRIMARY KEY`: Auto-incrementing integer. Every row gets a unique ID. Used for ordering and as a surrogate key.
- `event_id UNIQUE NOT NULL`: **Deduplication key**. If we try to insert a duplicate event_id, PostgreSQL rejects it. This prevents the same event from being stored twice.

**HOW**: `SERIAL` is a PostgreSQL shortcut that creates an integer column with an auto-incrementing sequence.

---

```sql
    event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
```

**WHAT**: Stores the event type with a CHECK constraint.

**WHY**:
- **Data integrity at DB level**. Even if application code has a bug, the database won't accept invalid event types.
- Restricts to exactly three valid values
- Catches errors early

**HOW**: CHECK constraints are evaluated before every INSERT/UPDATE. If the condition is false, the operation fails.

---

```sql
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
```

**WHAT**: Automatically records when each row was inserted.

**WHY**:
- **Auditing**: Know when data was ingested, not just when the event occurred
- `event_timestamp`: When the user clicked
- `created_at`: When it arrived in the database
- The difference = **ingestion latency**

**HOW**: `DEFAULT CURRENT_TIMESTAMP` means PostgreSQL fills this automatically if we don't provide a value.

---

```sql
CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_user_events_event_type ON user_events(event_type);
CREATE INDEX IF NOT EXISTS idx_user_events_timestamp ON user_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_user_events_type_timestamp ON user_events(event_type, event_timestamp);
```

**WHAT**: Creates indexes for faster queries.

**WHY each index**:
| Index | Use Case | Without Index |
|-------|----------|---------------|
| `user_id` | "Show all events for user X" | Scans every row |
| `event_type` | "Show all purchases" | Scans every row |
| `event_timestamp` | "Events in last hour" | Scans every row |
| `(event_type, event_timestamp)` | "Purchases in last hour" | Uses 2 separate indexes, less efficient |

**HOW**: Indexes are B-tree structures that allow O(log n) lookups instead of O(n) table scans. Trade-off: slower writes (index must be updated), faster reads.

---

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

**WHAT**: Creates a reusable view that aggregates event data.

**WHY**:
- **Simplification**: Users run `SELECT * FROM v_event_summary` instead of writing the complex query
- **Consistency**: Everyone uses the same calculation
- **Abstraction**: If calculation logic changes, update the view once

**HOW**: Views don't store data. They're saved queries that run when you SELECT from them. `CREATE OR REPLACE` allows updating without DROP/CREATE.

---

# Part 4: data_generator.py

## What Is It?
A Python script that simulates real e-commerce user activity by creating fake events and saving them as CSV files.

## Key Code with Explanations

```python
EVENT_TYPES = {
    'view': 0.6,        # 60% of events
    'add_to_cart': 0.3, # 30% of events
    'purchase': 0.1     # 10% of events
}
```

**WHAT**: Defines probability distribution for event types.

**WHY**: 
- Real e-commerce data follows a funnel pattern
- Many people browse (view), fewer add to cart, even fewer purchase
- This makes our test data realistic

**HOW**: When selecting an event type, we generate a random number 0-1 and check which bucket it falls into.

---

```python
def select_event_type() -> str:
    rand = random.random()  # Random number between 0 and 1
    cumulative = 0
    for event_type, probability in EVENT_TYPES.items():
        cumulative += probability
        if rand <= cumulative:
            return event_type
    return 'view'
```

**WHAT**: Picks an event type based on weighted probabilities.

**WHY**: We want 60% views, 30% add_to_cart, 10% purchase - not equal distribution.

**HOW**: 
1. Generate random number, e.g., 0.45
2. Check: 0.45 <= 0.6 (view threshold)? Yes → return 'view'
3. If rand was 0.72: 0.72 <= 0.6? No. 0.72 <= 0.9 (0.6+0.3)? Yes → return 'add_to_cart'

---

```python
def generate_event() -> Dict:
    return {
        'event_id': str(uuid.uuid4()),  # Universally unique ID
        'user_id': generate_user_id(),
        'product_id': generate_product_id(),
        'product_name': product_name,
        'category': category,
        'price': price,
        'event_type': select_event_type(),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
```

**WHAT**: Creates a single event dictionary.

**WHY**:
- `uuid.uuid4()`: **Guarantees uniqueness**. UUIDs are 128-bit identifiers with near-zero collision probability. This event_id is our deduplication key.
- Dictionary format matches what CSV writer expects

**HOW**: `uuid.uuid4()` generates a random UUID. `strftime` formats the datetime as a string that PostgreSQL can parse.

---

```python
class RateLimiter:
    def __init__(self, events_per_second: int):
        self.events_per_second = events_per_second
        
    def wait(self):
        if self.events_this_second >= self.events_per_second:
            sleep_time = 1.0 - (current_time - self.second_start)
            time.sleep(sleep_time)
            self.events_this_second = 0
```

**WHAT**: Controls how fast events are generated.

**WHY**:
- Without rate limiting, events generate as fast as CPU allows (thousands/second)
- We want controlled, realistic throughput (e.g., 10 events/second)
- Allows testing different load scenarios

**HOW**: 
1. Track how many events generated this second
2. If we hit the limit, calculate time remaining in this second
3. Sleep until the second ends
4. Reset counter, continue

---

```python
def write_events_to_csv(events: List[Dict], output_dir: str) -> str:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    filename = f"events_{timestamp}.csv"
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)
```

**WHAT**: Writes a batch of events to a CSV file with a unique name.

**WHY**:
- **Unique filename**: Uses timestamp + microseconds to prevent overwrites
- **CSV format**: Spark's file source can read CSV natively
- **newline=''**: Prevents extra blank lines on Windows
- **encoding='utf-8'**: Handles special characters

**HOW**: `DictWriter` maps dictionary keys to CSV columns. `writeheader()` creates the first row, `writerows()` writes all events.

---

```python
def signal_handler(signum, frame):
    global shutdown_requested
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
```

**WHAT**: Catches Ctrl+C and sets a flag.

**WHY**:
- **Graceful shutdown**: Without this, Ctrl+C kills the process immediately
- With this, we finish the current batch before exiting
- Prevents corrupted/partial CSV files

**HOW**: `signal.signal` registers a function to call when SIGINT (Ctrl+C) is received. We set a flag that the main loop checks.

---

# Part 5: spark_streaming_to_postgres.py

## What Is It?
The core Spark Structured Streaming application that reads CSV files, validates/transforms data, and writes to PostgreSQL.

## Key Code with Explanations

```python
class Config:
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
```

**WHAT**: Configuration class with environment variable fallbacks.

**WHY**:
- `os.getenv('POSTGRES_HOST', 'postgres')`: First tries environment variable, falls back to 'postgres'
- Inside Docker, 'postgres' is the service name which resolves to the PostgreSQL container's IP
- Outside Docker, you could set `POSTGRES_HOST=localhost`

**HOW**: Docker DNS resolves service names to container IPs on the same network.

---

```python
def get_event_schema() -> StructType:
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("price", DoubleType(), True),
        ...
    ])
```

**WHAT**: Defines the expected structure of CSV data.

**WHY**:
- **Performance**: Without explicit schema, Spark reads the file once to infer types, then again to process. With schema, it reads once.
- **Type safety**: Ensures `price` is treated as a number, not text
- **Best practice**: Always define schema for production streaming jobs

**HOW**: `StructType` is Spark's way of defining a schema. Each `StructField` specifies column name, data type, and whether nulls are allowed.

---

```python
spark = (SparkSession.builder
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
```

**WHAT**: Creates and configures the Spark session.

**WHY**:
- `spark.jars`: Points to the JDBC driver we downloaded in Dockerfile
- `spark.sql.adaptive.enabled`: Spark dynamically optimizes query plans based on data size
- `spark.sql.shuffle.partitions`: Default is 200, overkill for small data. 4 reduces overhead.

**HOW**: `SparkSession.builder` uses the builder pattern. Chain configurations, call `getOrCreate()` at the end.

---

```python
def clean_and_validate_data(df: DataFrame) -> tuple:
    # Normalize event_type to lowercase
    cleaned_df = cleaned_df.withColumn('event_type', lower(col('event_type')))
    
    # Add validation flag
    validated_df = cleaned_df.withColumn(
        'is_valid',
        (
            col('event_id').isNotNull() &
            col('event_type').isin(['view', 'add_to_cart', 'purchase']) &
            (col('price') > 0)
        )
    )
    
    valid_df = validated_df.filter(col('is_valid') == True)
    invalid_df = validated_df.filter(col('is_valid') == False)
    return valid_df, invalid_df
```

**WHAT**: Cleans data and splits into valid/invalid records.

**WHY**:
- **Data quality**: Bad data shouldn't reach the database
- **Debugging**: Invalid records saved separately for analysis
- **Case normalization**: 'VIEW', 'View', 'view' all become 'view'

**HOW**: 
1. Apply transformations using `withColumn`
2. Create boolean column `is_valid` based on rules
3. Filter into two DataFrames based on that column
4. Return both

---

```python
def write_to_postgres_with_retry(batch_df: DataFrame, batch_id: int):
    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            final_df.write \
                .format("jdbc") \
                .option("url", Config.JDBC_URL) \
                .option("dbtable", "user_events") \
                .mode("append") \
                .save()
            return  # Success, exit
        except Exception as e:
            if "duplicate key" in str(e).lower():
                return  # Duplicates are expected, not an error
            if attempt < Config.MAX_RETRIES:
                time.sleep(Config.RETRY_DELAY_SECONDS)
            else:
                raise  # All retries failed
```

**WHAT**: Writes data to PostgreSQL with retry logic.

**WHY**:
- **Transient failures**: Network glitches, temporary DB unavailability happen
- **Retry logic**: Automatically recovers from temporary issues
- **Duplicate handling**: Database UNIQUE constraint rejects duplicates. This is expected, not an error.
- `mode("append")`: Adds rows without deleting existing data

**HOW**:
1. Attempt to write via JDBC
2. If success, return
3. If failure, check if it's a duplicate (expected)
4. Otherwise, wait and retry up to MAX_RETRIES times
5. If still failing, raise exception (Spark will retry the batch)

---

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

**WHAT**: Starts the streaming query.

**WHY each option**:

| Option | Purpose |
|--------|---------|
| `foreachBatch` | Calls our custom function for each micro-batch. Gives us full control over write logic. |
| `outputMode("append")` | Output only new rows. Alternative: "complete" outputs everything each time. |
| `trigger(processingTime="10 seconds")` | Check for new files every 10 seconds. Batches data for efficiency. |
| `checkpointLocation` | **Critical for fault tolerance**. Stores which files/offsets were processed. On restart, resumes from checkpoint. |

**HOW**: Spark polls the input directory every trigger interval. New files form a micro-batch. The batch flows through our function, gets written to PostgreSQL, and the checkpoint is updated.

---

# Part 6: Data Flow Summary

```
[1] data_generator.py creates events_20260130_120000.csv
                ↓
[2] Spark watches /app/data/raw/, sees new file
                ↓
[3] Spark reads CSV, validates records
    ├── Valid → Continue processing
    └── Invalid → Write to /app/data/error/
                ↓
[4] Deduplicate within batch (dropDuplicates)
                ↓
[5] Write to PostgreSQL via JDBC
    ├── Success → Update checkpoint
    └── Duplicate key → Silently skip (expected)
                ↓
[6] Checkpoint updated, wait for next trigger interval
```

---

# Part 7: Demo Script

```bash
# Terminal 1: Start infrastructure
docker compose up -d --build

# Terminal 2: Start streaming job (watch the logs)
docker compose exec spark spark-submit //app/src/spark_streaming_to_postgres.py

# Terminal 3: Generate events
docker compose exec spark python3 //app/src/data_generator.py --events-per-second 10 --duration 30

# Terminal 4: Query results
docker compose exec postgres psql -U postgres -d ecommerce_events -c "SELECT * FROM v_event_summary;"
```

---

# Part 8: Common Questions

**Q: What happens if the Spark job crashes mid-batch?**
A: On restart, Spark reads the checkpoint and reprocesses the incomplete batch. The database UNIQUE constraint prevents duplicate inserts.

**Q: Why not use Kafka instead of files?**
A: Files are simpler for demo. The same code works with Kafka by changing `readStream.format("csv")` to `readStream.format("kafka")`.

**Q: How would you scale this for production?**
A: 
1. Replace file source with Kafka for higher throughput
2. Increase Spark executors for parallel processing
3. Partition PostgreSQL table by timestamp
4. Use connection pooling for database writes
