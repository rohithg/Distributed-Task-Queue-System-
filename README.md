# Distributed Task Queue System (Python)

Production-grade asynchronous task queue with priority support, retry logic, and worker pool management.

## Features

- **Async/Await**: Modern Python concurrency with asyncio
- **Priority Queue**: Tasks executed based on priority levels
- **Worker Pool**: Configurable number of concurrent workers
- **Retry Logic**: Exponential backoff for failed tasks
- **Task Tracking**: Complete lifecycle tracking (pending → running → completed/failed)
- **Statistics**: Real-time metrics and success rates
- **Type Hints**: Full type annotations for better code quality
- **Logging**: Comprehensive logging for debugging

## Technical Highlights

### Design Patterns
- **Producer-Consumer**: Queue-based task distribution
- **Worker Pool**: Multiple workers processing tasks concurrently
- **Strategy Pattern**: Pluggable task execution strategies

### Python Features Demonstrated
- Asyncio and coroutines
- Type hints and generics
- Enums and dataclasses
- Context managers
- Exception handling
- Logging

### Concurrency Concepts
- Async/await syntax
- Event loops
- Futures and tasks
- Concurrent execution
- Thread pool executors

## Architecture

```
┌─────────────────┐
│   Task Submit   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Priority Queue  │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌────────┐ ┌────────┐
│Worker 1│ │Worker 2│ ... Worker N
└────────┘ └────────┘
    │         │
    └────┬────┘
         ▼
    ┌─────────┐
    │ Results │
    └─────────┘
```

## Usage

```python
# Create queue with 4 workers
queue = TaskQueue(num_workers=4)
await queue.start()

# Submit tasks with different priorities
task_id = await queue.submit(
    my_function,
    arg1, arg2,
    priority=10,  # Higher number = higher priority
    max_retries=3
)

# Check task status
status = queue.get_task_status(task_id)

# Get statistics
stats = queue.get_stats()

# Graceful shutdown
await queue.stop()
```

## Running

```bash
python task_queue.py
```

## Real-World Applications

- **Background Jobs**: Email sending, report generation
- **Data Processing**: ETL pipelines, batch processing
- **Microservices**: Async task execution between services
- **Web Scraping**: Distributed crawling with rate limiting
- **Image Processing**: Thumbnail generation, video encoding

## Extensions

Can be extended with:
- Redis backend for distributed processing
- Dead letter queue for failed tasks
- Task scheduling (cron-like)
- Rate limiting
- Task dependencies and workflows
- Monitoring dashboard

## Interview Topics

- Asyncio and event loops
- Concurrency vs parallelism
- Producer-consumer pattern
- Error handling and retries
- Resource management
- Distributed systems concepts
