![bugfruit.png](bugfruit.png)

# bugfruit

bugfruit is a simple embedded key-value store.

## About
- Supports single writer, multiple concurrent readers.
- Configurable `fsync` and garbage collection batch size.
- Point-in-time snapshots.

## Usage
To use in your application: `go get github.com/reesporte/bugfruit`

```go
s, err := bugfruit.NewStorage(
    "mealtime.db",
    0644,
    &bugfruit.Config{
        VacuumBatch: 3600, // 3600 writes before garbage collection
        FsyncBatch:  2000, // 2000 writes before fsync
    },
)
if err != nil {
    log.Fatalf("couldn't create storage: %v", err)
}

err = s.Set(
    "Meriadoc",
    []byte("I don't think he knows about second breakfast, Pip."),
)
if err != nil {
    log.Fatalf("couldn't set: %v", err)
}

if val, ok := s.Get("Meriadoc"); ok {
    fmt.Printf("\"%s\", said Meriadoc.\n", val)
}

if err = s.Delete("Meriadoc"); err != nil {
    log.Fatalf("couldn't delete: %v", err)
}

err = s.Set(
    "Pippin",
    []byte("What about elevenses? Luncheon? Afternoon tea? Dinner? Supper?"),
)
if err != nil {
    log.Fatalf("couldn't set: %v", err)
}

if err = s.Snapshot("mealtime-snapshot.db", 0644); err != nil {
    log.Fatalf("couldn't take snapshot: %v", err)
}
```

## Performance
### Benchmarks
I approximately replicated some baseline LMDB microbenchmarks found
[here](http://www.lmdb.tech/bench/microbench/) for random reads and random writes.

I ran these benchmarks on an M1 Mac with 8GB of RAM and GOMAXPROCS set to 4 to
replicate the 4 core setup the LMDB microbenchmarks used. Like in the LMDB
benchmarks, `fsync` and garbage collection were turned off. 

Keep in mind that there's
[3 kinds of lies](https://en.wikipedia.org/wiki/Lies,_damned_lies,_and_statistics):
lies, damned lies, and benchmarks. I am running on more modern hardware than the
benchmarks I've linked to. The LMDB benchmarks are from September 2012 running on
devices built in 2012, whereas mine are running on a 2020 M1 Macbook Air.

Also, while somewhat useful for comparing databases, these
benchmarks don't necessarily reflect performance in a real production environment on
your hardware. They should be taken with that healthy grain of salt.

Benchmarking code can be found in `benchmarks/benchmark.go`.

#### Small values: random 16 byte keys, random 100 byte values
For comparison, the relevant LMDB microbenchmarks are
[here](http://www.lmdb.tech/bench/microbench/#sec2).

##### 1,000,000 ops per iteration
I ran 1,000,000 operations per iteration, the same as the LMDB microbenchmarks.
The output file is `benchmarks/bench-x-small.log`.

|Type|Average ops/sec| Std. Dev. ops/sec|
|---|---|---|
|Set|417,070|7,953|
|Delete|583,134|7,283|
|Get|7,891,966|210,693|

##### 10,000,000 ops per iteration
For the sake of completeness (and curiosity), I also ran 10,000,000 operations per iteration.
The output file is `benchmarks/bench-small.log`.

|Type|Average ops/sec| Std. Dev. ops/sec|
|---|---|---|
|Set|360,777|3,100|
|Delete|466,481|4,168|
|Get|6,171,403|174,074|

#### Large values: random 16 byte keys, random 100,000 byte values
For comparison, the relevant LMDB microbenchmarks are
[here](http://www.lmdb.tech/bench/microbench/#sec4).

##### 10,000 ops per iteration
I ran 10,000 operations per iteration, the same as the LMDB microbenchmarks.
The output file is `benchmarks/bench-large.log`.

|Type|Average ops/sec| Std. Dev. ops/sec|
|---|---|---|
|Set|10,783|318|
|Delete|153,036|81,142|
|Get|20,864,490|809,940|

##### 100,000 ops per iteration
I also ran 100,000 operations per iteration. Unsurprisingly, the increased number of
operations per iteration decreased performance, especially since the total database
size ended up being 2GB larger than my RAM (~10GB vs 8GB).

Despite the decrease in performance, it was still fairly on par with other key-value
databases in the less intensive LMDB microbenchmarks. 

You can see the benchmark output files in `benchmarks/bench-x-large.log`.

|Type|Average ops/sec| Std. Dev. ops/sec|
|---|---|---|
|Set|4,500|1,019|
|Delete|5,194|411|
|Get|11,641,740|2,572,501|

### Disk Usage
bugfruit does not compress data, which can result in a large database file,
especially if you don't run garbage collection.

To calculate how large your database file will be, sum the size of your
key/value pair in bytes with 9 (the size of a datum's metadata). If you delete a
datum but don't run garbage collection, that datum's size should still be included
in the total size of the database. Likewise, if you overwrite a datum but don't run
garbage collection, the old datum's size should be included in the total size of the
database.

### Limitations
bugfruit has various limitations that may exclude it from being a viable choice for
your application. These include:
- All data should fit in RAM. If your machine does not have enough RAM to comfortably
  fit all your data, you may notice performance issues.
- All operations are individual transactions. Any get/set/delete/snapshot operation
  happens atomically. There is no support for rollbacks, batched transactions, or
  reads/writes within the same transaction.
- bugfruit is optimized for read performance over write performance. While it can
  perform fairly well for a moderate number of writes, it is not intended for use
  with predominantly write-heavy applications.
