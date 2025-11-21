# mini-redis-from-first-principal

[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/)

<p> A minimal Redis like system implemented from scratch in Python supporting the main Redis functionalities such as `SET`, `GET`, `DEL`, `EXPIRE`, lists, and pub/sub, Backup/recovery, multi-client concurrency.</p>
<p>This project is implemented while doing the courseworks of Backend Engineering Course.</p>
<p>I would like to express my special gratitude towards shimanta paul for his guidance throughout this project.</p>

## Features that are included so far:
- Single threaded event loop based multi client concurrency.
- Core Redis commands:
  - `SET`, `GET`, `DEL`, `EXPIRE`
  - List operations: `LPUSH`, `LPOP`
  - SET, HASH Data structure operations.
  - Pub/Sub: `PUBLISH`, `SUBSCRIBE`
- TTL , PTTL implementation + lazy expiration.
- Redis Native Data structures.
- RDB and AOF backup/snapshots + recovery on startup.
- TCP server that can be connected via **telnet** or programmatically
- Automated tests with **pytest**
---

## Experience while doing the project + learnings: 
- It took almost 1.5 months to complete this project while maintaing a full time job. Although I could have finished it earlier, but I was doing multiple other personal projects which are focused towards MLOPS and ML/ Deep Learning research. In Each weeknd I implemeted a specific feature which was maintainted in there corresponding git branch, if you are interested in specifc feature you can find it in the corresponding branch of the github repo.
- Tried to learn in depth about python GIL/Concurrency (threading), multi client handling etc.
- Learned some new design patterns and tried to ensure modularized coding structure as much as possible.
---
## Installation

### Clone the repository
```bash
git clone https://github.com/mahi-anol/Mini-Redis-Coded-from-Scratch.git
```
## Go to project dir
```
cd Mini-Redis-Coded-from-Scratch.git
```
## Installing virtual Environment (optional)
```
# Windows
python -m venv .venv
.\.venv\Scripts\activate

# Linux/macOS
python3 -m venv .venv
source .venv/bin/activate
```
## Installing package
```
# Windows
pip install .
# linux
pip3 install .
```
## Now open 2 separate terminal instance
```
##  In first terminal do:
# For Windows
python main.py

# For linux
python3 main.py
```
## Now in other terminal do:
```
telnet localhost 6379
# After this we are connected to the server.
```
## Now we can try redis commands
```
PING
SET A 5
GET A
LPUSH B a b c d e
GET B
```
<p> when you stop the server and restart again you'll see that the server is storing the state from backups and and key value pair will be restored along with their meta data. </p>
<p> Also you can check multi client concurrency with by connecting multiple telnet seesion from different terminals.
---

## TODOs:
- Implementing the pattern based PUB SUB methods.
- creating better documentation.

## Author
**Mahi Sarwar Anol**
- Email: anol.mahi@gmail.com  
- GitHub: [mahi-anol](https://github.com/mahi-anol)  
- LinkedIn: [Mahi Sarwar Anol](https://www.linkedin.com/in/mahi-anol)  