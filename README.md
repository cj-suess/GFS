# Distributed, Replicated, and Fault-Tolerant File System

The objective of this project is to build a distributed, failure-resilient file system. Fault tolerance is achieved through **replication**, ensuring files are dispersed over a set of available machines and remain accessible even in the event of node failures or data corruption.

## High-Level Overview

The system consists of three main components responsible for managing file storage, maintaining metadata, and handling client requests.

### 1. Controller Node
* **Role:** Acts as the central manager for the system. There is only one active instance of the controller.
* **Metadata Management:** Tracks information about live chunk servers and the chunks they hold. This information is maintained strictly in memory; the controller does not store metadata on the disk.
* **Failure Detection:** Detects chunk server failures via missing heartbeats and initiates recovery procedures.

### 2. Chunk Server
* **Role:** Responsible for managing and storing file chunks. Each machine hosts one chunk server instance.
* **Storage:** Stores chunks as regular files on the local disk.
* **Integrity:** Maintains SHA-1 checksums for every 8KB slice of a chunk to detect tampering or corruption.
* **Heartbeats:** Periodically sends messages to the Controller:
    * **Major Heartbeat (every 60s):** Sends metadata about *all* chunks and free space.
    * **Minor Heartbeat (every 15s):** Sends information about *newly added* chunks.

### 3. Client
* **Role:** Handles file storage (uploading) and retrieval (downloading).
* **File Splitting:** Splits files into **64KB chunks** before distribution.
* **Pipelined Writing:** To utilize bandwidth efficiently, the client contacts the Controller for a list of 3 chunk servers but writes data only to the first. The first server forwards to the second, which forwards to the third.

---

## Core Mechanisms

### Data Replication
* **Replication Level:** Each file chunk is replicated 3 times.
* **Placement:** No single chunk server holds more than one replica of the same chunk.
* **Data Flow:** Data flows directly between the Client and Chunk Servers (or between Chunk Servers). Data **never** flows through the Controller.

### Fault Tolerance & Recovery
* **Corruption Detection:** If a Chunk Server detects a SHA-1 mismatch (corruption) during a read, it notifies the Controller.
* **Self-Healing:** Upon detecting corruption, the system contacts valid replicas to fix the corrupted chunk slice.
* **Server Failure:** If a Chunk Server fails (stops sending heartbeats), the Controller designates other servers to create new replicas to maintain the replication level.
