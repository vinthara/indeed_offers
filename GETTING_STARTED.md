# Setting up PostgreSQL

## Install PostgreSQL

To get started you'll need a PostgreSQL server. You can [install](https://www.postgresql.org/download/) it locally. Or on  a virtual machine. 

You can get a free virtual machine with high specs on [Oracle cloud](https://cloud.oracle.com/compute/instances), however it uses `aarch64` architecture, but it works perfectly fine with PostgreSQL.

## Create an user

Make sure you start and enable the `PostgreSQL server` :

```shell
systemctl enable postgresql
```

Connect to `PostgreSQL` :

```shell
sudo -i -u postgres
```

Type in :

```shell
psql
```

You should now get a prompt like this :

```shell
postgres=#
```

Replace `<...>` accordingly :

```
CREATE USER <name> WITH PASSWORD '<password>';
CREATE DATABASE indeed;
```

You can always alter the password later :

```
ALTER USER <name> WITH PASSWORD '<password>';
```

Grant privileges on database to user :

```
GRANT ALL PRIVILEGES ON DATABASE <dbname> TO <user>;
```

Exit psql : 

```shell
exit
```

## Allow user to connect to PostgreSQL

Naviage to `/etc/postgres/14/main` or newer version :

Edit the `pg_hba.conf` and add the line under `# IPv4` :

```
host    all             <user>             0.0.0.0/0               md5
```

Also edit `postgresql.conf` file, so you can connect from any address :

```
listen_addresses = '*' 
```

## Create `.pg_service.conf` service connection file

Edit the `.pg_service_sample.conf` file accordingly : 

```
[offers]
user=user
password=password
dbname=indeed
host=xxx.xxx.xxx.xxx
port=5432
```

Move the file :

```shell
mv .pg_service_sample.conf .pg_service.conf
```

Export the environment variable :

```shell
export PGSERVICEFILE=.pg_service.conf
```

Test that everything works :

```
psql "service=offers_bis" -c "SELECT now();"
```

Edit `postgresql_credentials_sample.json` accordingly : 

```json
{
    "username":"user",
    "password":"password",
    "database":"indeed",
    "host":"xxx.xxx.xxx.xxx"
}
```

Move the file : 

```shell
mv postgresql_credentials_sample.json postgresql_credentials.json
```

