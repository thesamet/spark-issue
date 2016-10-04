Minimal example for https://issues.apache.org/jira/browse/SPARK-17766

Steps to reproduce:

1. I put the code in a repository: `git clone https://github.com/thesamet/spark-issue`
2. in one terminal: `while true; do nc -l localhost 9999; done`
3. Start a new terminal
4. Run "sbt run".
5. Type a few lines in the netcat terminal.
6. Kill the streaming project (Ctrl-C), 
7. Go back to step 4 until you see the exception above.

I tried the above with local filesystem and also with S3, and getting the same result.
