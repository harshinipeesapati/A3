LozonDB – Hash-Based Distributed Cloud Storage (EDM1 A3)

LozonDB is a hash-based distributed key–value storage system implemented as part of the EDM1 “Cloud Storage Systems” assignment.  
It uses a coordinator and four independent bucket processes that communicate via TCP sockets and persist data to separate disk files.  
The client application loads a Kaggle dataset, exercises the full dictionary API, and generates detailed logs.



Features

- Hash-based distributed access structure with memory-resident directory  
- Four independent bucket processes, each with its own persistent file  
- Dictionary API: `insert(k,v)`, `search(k)`, `delete(k)`, `range(k1,k2)`  
- Client application that:
  - Loads `genome_scores.csv` (Kaggle MovieLens dataset, ~10K inserts)
  - Performs random reads across all four buckets
  - Deletes at least two keys and verifies deletion
  - Executes a multi-bucket range query
- Detailed logging with timestamps, node ID, filename, and key–value pairs  
- CAP analysis: mostly CP (Consistency + Partition tolerance) in a distributed setting

Repository Structure

lozon_hash_store/
├── client_test.py        # Client + automated test suite
├── coordinator.py        # Coordinator / hash directory (TCP:9000)
├── bucket_server.py      # Bucket process implementation (TCP:9100–9103)
├── config.py             # Configuration (ports, paths, NUM_BUCKETS, etc.)
├── data/                 # Persistent bucket files (created at runtime)
├── logs/                 # run.log + test_report.log (created at runtime)
├── documentation.pdf     # Assignment report
└── README.md             # This file
