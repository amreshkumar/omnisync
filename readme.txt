Installing dependencies:
- Run `pip install -r requirements.txt`

Looking on Docker container for logs:
1. `docker build -t omnisync .`
2. `docker run -d -p 4000:80 omnisync`
3. `docker ps`
4. `docker exec -it container_id /bin/bash`
5. `cat s3_sync.log`

Documentation:
1. Introduction (1 page)
2. Data Mapping and Tracking: using boto library for AWS and schedule library to run the job every set interval (2-3 page)
3. Adaptive Synchronization: Custom delete rules, S3 versioning, two-way-sync. (3 page)
4. Conflict Resolution: Using S3 versioning (1 page)
5. User Interface: Add screenshots from S3/AWS. (2-3 pages)
6. Docker file: Virtualization using docker (2 pages)
7. Team intro - 1 page
8. Future work (out of scope work) - 1 page
