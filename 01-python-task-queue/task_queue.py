"""
Distributed Task Queue System with Redis Backend
Demonstrates: Concurrency, distributed systems, async/await, design patterns
"""

import asyncio
import json
import time
import uuid
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, Any, Optional, List
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class Task:
    """Represents a task in the queue"""
    
    def __init__(self, func: Callable, *args, **kwargs):
        self.id = str(uuid.uuid4())
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.status = TaskStatus.PENDING
        self.result = None
        self.error = None
        self.created_at = datetime.now()
        self.started_at = None
        self.completed_at = None
        self.retry_count = 0
        self.max_retries = kwargs.get('max_retries', 3)
        self.priority = kwargs.get('priority', 0)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize task to dictionary"""
        return {
            'id': self.id,
            'func_name': self.func.__name__,
            'status': self.status.value,
            'result': self.result,
            'error': str(self.error) if self.error else None,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'retry_count': self.retry_count,
            'priority': self.priority
        }


class TaskQueue:
    """
    Distributed task queue with priority support and retry logic
    Uses in-memory storage (can be extended with Redis)
    """
    
    def __init__(self, num_workers: int = 4):
        self.tasks: Dict[str, Task] = {}
        self.pending_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.num_workers = num_workers
        self.workers: List[asyncio.Task] = []
        self.running = False
        self.stats = {
            'total_tasks': 0,
            'completed': 0,
            'failed': 0,
            'retried': 0
        }
    
    async def submit(self, func: Callable, *args, **kwargs) -> str:
        """Submit a new task to the queue"""
        task = Task(func, *args, **kwargs)
        self.tasks[task.id] = task
        self.stats['total_tasks'] += 1
        
        # Priority queue: lower number = higher priority
        await self.pending_queue.put((-task.priority, task.id))
        logger.info(f"Task {task.id} submitted (priority: {task.priority})")
        
        return task.id
    
    async def worker(self, worker_id: int):
        """Worker coroutine that processes tasks"""
        logger.info(f"Worker {worker_id} started")
        
        while self.running:
            try:
                # Get next task (with timeout to check running flag)
                priority, task_id = await asyncio.wait_for(
                    self.pending_queue.get(),
                    timeout=1.0
                )
                
                task = self.tasks.get(task_id)
                if not task:
                    continue
                
                # Execute task
                await self._execute_task(task, worker_id)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
    
    async def _execute_task(self, task: Task, worker_id: int):
        """Execute a single task with retry logic"""
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()
        
        logger.info(f"Worker {worker_id} executing task {task.id}")
        
        try:
            # Execute the function (support both sync and async)
            if asyncio.iscoroutinefunction(task.func):
                result = await task.func(*task.args, **task.kwargs)
            else:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, task.func, *task.args, **task.kwargs
                )
            
            # Task succeeded
            task.result = result
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            self.stats['completed'] += 1
            
            logger.info(f"Task {task.id} completed successfully")
            
        except Exception as e:
            logger.error(f"Task {task.id} failed: {e}")
            task.error = e
            
            # Retry logic
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.RETRYING
                self.stats['retried'] += 1
                
                # Re-queue with exponential backoff
                await asyncio.sleep(2 ** task.retry_count)
                await self.pending_queue.put((-task.priority, task.id))
                
                logger.info(f"Task {task.id} retrying (attempt {task.retry_count})")
            else:
                # Max retries reached
                task.status = TaskStatus.FAILED
                task.completed_at = datetime.now()
                self.stats['failed'] += 1
                
                logger.error(f"Task {task.id} failed permanently after {task.retry_count} retries")
    
    async def start(self):
        """Start the task queue and workers"""
        self.running = True
        self.workers = [
            asyncio.create_task(self.worker(i))
            for i in range(self.num_workers)
        ]
        logger.info(f"Task queue started with {self.num_workers} workers")
    
    async def stop(self):
        """Stop the task queue gracefully"""
        logger.info("Stopping task queue...")
        self.running = False
        
        # Wait for all workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        logger.info("Task queue stopped")
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific task"""
        task = self.tasks.get(task_id)
        return task.to_dict() if task else None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return {
            **self.stats,
            'pending': self.pending_queue.qsize(),
            'success_rate': (
                self.stats['completed'] / self.stats['total_tasks'] * 100
                if self.stats['total_tasks'] > 0 else 0
            )
        }


# Example task functions
async def fetch_data(url: str, delay: float = 1.0) -> Dict[str, Any]:
    """Simulate fetching data from an API"""
    await asyncio.sleep(delay)
    return {
        'url': url,
        'data': f"Data from {url}",
        'timestamp': datetime.now().isoformat()
    }


def process_data(data: str) -> str:
    """Simulate CPU-intensive data processing"""
    time.sleep(0.5)  # Simulate work
    return data.upper()


async def send_email(to: str, subject: str, body: str) -> bool:
    """Simulate sending an email"""
    await asyncio.sleep(0.3)
    logger.info(f"Email sent to {to}: {subject}")
    return True


# Demo application
async def main():
    """Demonstration of the task queue system"""
    queue = TaskQueue(num_workers=3)
    await queue.start()
    
    # Submit various tasks with different priorities
    task_ids = []
    
    # High priority tasks
    for i in range(5):
        task_id = await queue.submit(
            fetch_data, 
            f"https://api.example.com/data/{i}",
            delay=0.5,
            priority=10  # High priority
        )
        task_ids.append(task_id)
    
    # Medium priority tasks
    for i in range(5):
        task_id = await queue.submit(
            process_data,
            f"data_{i}",
            priority=5
        )
        task_ids.append(task_id)
    
    # Low priority tasks
    for i in range(5):
        task_id = await queue.submit(
            send_email,
            f"user{i}@example.com",
            "Test Email",
            "This is a test",
            priority=1
        )
        task_ids.append(task_id)
    
    # Wait for all tasks to complete
    await asyncio.sleep(5)
    
    # Print results
    print("\n" + "="*60)
    print("TASK QUEUE RESULTS")
    print("="*60)
    
    for task_id in task_ids[:3]:  # Show first 3 tasks
        status = queue.get_task_status(task_id)
        if status:
            print(f"\nTask: {status['id'][:8]}...")
            print(f"  Status: {status['status']}")
            print(f"  Function: {status['func_name']}")
            print(f"  Result: {status['result']}")
    
    # Print statistics
    stats = queue.get_stats()
    print(f"\n{'='*60}")
    print("STATISTICS")
    print("="*60)
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').title()}: {value}")
    
    await queue.stop()


if __name__ == "__main__":
    asyncio.run(main())
