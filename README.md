Algorand trade&price indexer written in multithreaded Python that works forward, backwards, track volumes, tx number, and works with tinyman, pactfi, algofi and humble.

Looking for refining it, and supporting several options in one file (especially using different databases or not using it at all) Very early yet, work in progress


mysql bootstrap (copy and paste on fresh installation):

create user 'pablo'@'localhost' identified by 'algocharts'; create database algocharts; grant all privileges on algocharts.* to 'pablo'@'localhost'; flush privileges; use algocharts; create table pools (asa1 bigint unsigned not null, asa2 bigint unsigned not null, pool VARCHAR(64) not null PRIMARY KEY, market tinyint unsigned not null, lptoken bigint unsigned not null, liqa1 bigint unsigned, liqa2 bigint unsigned);
