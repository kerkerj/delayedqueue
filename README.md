# delayedqueue

Delayed queue with Redis implementation 

ref: https://redislabs.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-4-task-queues/6-4-2-delayed-tasks/

Basic concept:
1. User redis `sorted set` as the `delayed queue`
2. When enqueuing data to the delayed queue, we use execution timestamp as the score, 
   e.g. use 1620199248 as the score (`ZSet`)
3. When de-queuing data, we use `ZRangeByScore` and `ZRem` with current timestamp to check if there is data.
4. When there is nothing in the queue, sleep for a period.

```
Workflow Chart:
+----------+           +--------------+                   +--------------+
|          |           |              |                   |              |
|  Caller  |           | DelayedQueue |                   | WorkerQueue  |
|          |           | Consumer     +----------------+  | Consumers    |
|          |           |              |                |  |              |
+----+-----+           +---+-^--------+                |  +-----+-^------+
     |                     | |                         |        | |
     |1.Add jobs with      | |2.Consume and add jobs   |        | |3.Consume and process the jobs
     | a delayed time      | | to working queue        |        | |
     |                     | |                         |        | |
+----v---------------------v-+-----------------------+ |  +-----v-+-----------------------------------+
|                                                    | |  |                                           |
|  DelayedQueue(Redis Sorted Set)                    | +-->  WorkingQueue(Redis List)[work_q]         |
| +-----------+------------------------------------+ |    | +------------------------------------+    |
| | score     | member                             | |    | | member                             |    |
| |(timestamp)|[queueName,funcName,{"arg1":"1"}]   | |    | |[queueName,funcName,{"arg1":"1"}]   |    |
| +-----------+------------------------------------+ |    | +------------------------------------+    |
| |1620299103 |["work_q","CallAPI","{"arg1":"1"}"] | |    | |["work_q","CallAPI","{"arg1":"1"}"] |    |
| +-----------+------------------------------------+ |    | +------------------------------------+    |
| |1620100100 | ...                                | |    | | ...                                |    |
| +-----------+------------------------------------+ |    | +------------------------------------+    |
|                                                    |    |                                           |
+----------------------------------------------------+    +-------------------------------------------+

Chart created by: https://asciiflow.com/
```

