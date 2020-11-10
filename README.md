# gdc-data-exercise

Small Python app to clean CSV files and insert it to a Postgresql database.

Time spent : 5 hours

### Prerequisites

You need `docker` and `docker-compose` to run this app. You can follow this [link](https://docs.docker.com/compose/install/) to install (but Docker 19.03.0+ is needed).

### Installing

```
git clone https://github.com/nicolazg/gdc-data-exercise.git
cd gdc-data-exercice/
docker-compose build
```

### Running

```
docker-compose up app
```

### Technical choices

__Postgresql__
* object-relational database open source
* high concurrency, ACID compliance
* adheres more closely to SQL standards

__SQLAlchemy__

I made the choice to have a more generic brick at the cost of performance.
As a result, it is easier to change the provider.
Nevertheless, using chunksize in `pandas.to_sql` method allows to simulate bulk insert and to obtain acceptable performances.

### Functionnal choices

I've chosen to separate users' connections in a separate table (and dropped misc from the user table) to better exploit this information. However, it takes a lot of time and makes the treatment more complicated.

### Datascience part

I didn't have enough time to process this part as I had planned.

My initial goal was to analyze the time between the publication of an ad and its transaction in order to identify a potential user alert suggesting to put his ad back on top of the pile.

Unfortunately, I only produced a graph showing the distribution of this delta by category (as shown below).

![chart](/data/time_to_be_sold.png)

### Testing

Install `pytest 6.1.2` and then run `pytest` at the root of the project for launching few tests.

### Next steps and optimizations
* Adding more tests (not enough time)
* Speeding up the application
  * optimizing some data treatment
  * using database driver and bulk insert
* Adding security to the database (schema)