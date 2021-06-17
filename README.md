# file-merger-utility
This utility helps to merge many part files into a single file

Often in big data jobs we come accross scenarios where a job has to scan huge number of partitions to get the data and write the scanned and anlysed data into local file system.
In such situation yarn takes lot of time to allocate the resources requested by a job which keeps the job in accepted state for long. Things get worse if there are multiple such jobs running simultaneously and the root queue is a shared by other jobs. In such scenarios we can split one macro job to multiple micro jobs which would require less resources and will be easy for yarn to allocate resources to those jobs. This would reduce the time spent by the jobs in accepted state. But this comes with a catch, the number of files generated when the job finishes will be more as each job will generate atleast one file provided we repartition the data into a single partition. Here is a utility in python to merge those files into a single file.


To split the files we can use a convention as below:
Let's assume we have a single file file-name.csv and this file will be split into 5 parts as 5 jobs will run in place of a single job. So the part files will be named as file-name_<total_parts>_<part_no.>.csv
For the above example the split files will be named as: file-name_5_1.csv, file-name_5_2.csv, file-name_5_3.csv, file-name_5_4.csv, file-name_5_5.csv.
The final file will be named as file-name_merged.csv which will have the data from all the files.

We have to take care of the edge case where out of 5 jobs only 3 jobs have completed, then the merger job should reject those files and pick them up in the next run to merge.
