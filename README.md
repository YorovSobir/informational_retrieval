# Crawler 1.0

## What will search?
treatment based on symptoms

## Special feature
advice on treatment

## How to run
1. Configure database server
2. Run crawler.py with argument "-h \<url\> -p \<port\> -d \<dbname\> -u \<user\>"
  \<url\> - database server's url
  \<port\> - database server's port
  \<user\> - user in database
  \<dbname\> - name of database in server

## Configure database server
1. Edit file '/etc/postgresql/\<version\>/main/postgresql.conf':
  change value listen_address='localhost' to address of your local network
2. Edit file '/etc/postgresql/\<version\>/main/pg_hba.conf':
  allowed connection from other address
  e.g host \<dbname\> \<dbuser\> \<address\> \<options\> (trust)
