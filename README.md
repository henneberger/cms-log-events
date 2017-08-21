# cms-log-events
Finds top-10 'good' log events and their cumulative size transferred. Good log events are 2xx GET events.

For example, it will take a log:
```
[01/Aug/1995:00:54:59 -0400] "GET /images/opf-logo.gif HTTP/1.0" 200 32511
[01/Aug/1995:00:55:04 -0400] "GET /images/ksclogosmall.gif HTTP/1.0" 200 3635
[01/Aug/1995:00:55:06 -0400] "GET /images/ksclogosmall.gif HTTP/1.0" 403 298
[01/Aug/1995:00:55:09 -0400] "GET /images/ksclogosmall.gif HTTP/1.0" 200 3635
[01/Aug/1995:00:55:18 -0400] "GET /images/opf-logo.gif HTTP/1.0" 200 32511
[01/Aug/1995:00:56:52 -0400] "GET /images/ksclogosmall.gif HTTP/1.0" 200 3635
```

And returns the results in order of their frequency:
```
/images/ksclogosmall.gif 10905
/images/opf-logo.gif 65022
```


## Probabilistic trade-off
Has the option of using a probabilistic approach if the log cannot be streamed into memory.  It will only keep the top-k results in memory and will use two instances of count-min-sketches to individually determine frequency estimation and total size.

Extremely high cardinality log events (many unique GET URIs) with high volume will have lower accuracy due to saturation of the sketch. Different tuning parameters will need to be used to accommodate this situation. 

## Tests
There exists a log generator to generate log events with tunable cardinality.

The probabilistic approach is measured by an F1 score of the top-k results.

## Issues
The MinMaxPriorityQueue removal O(k) because I didn't find a clean way to remove non-distinct elements within the time-box of this task.
