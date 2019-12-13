declare module Highland {
    interface Stream<R> extends NodeJS.EventEmitter {

    /**
     * 
     * Takes a Stream of Streams and merges their values and errors into a 
     * single new Stream, limitting the number of unpaused streams that can 
     * running at any one time.
     *
     * Note that no guarantee is made with respect to the order in which
     * values for each stream end up in the merged stream. Values in the
     * merged stream will, however, respect the order they were emitted from
     * their respective streams.
     *
     * @id merge
     * @section Streams
     * @name Stream.mergeWithLimit()
     * @param {Number} n - integer representing number of values to read from source
     * @api public
     *
     * var txt = _(['foo.txt', 'bar.txt']).map(readFile)
     * var md = _(['baz.md']).map(readFile)
     * var js = _(['bosh.js']).flatMap(readFile)
     *
     * _([txt, md, js]).mergeWithLimit(2);
     * // => contents of foo.txt, bar.txt, baz.txt and bosh.js in the order
     * // they were read, but bosh.js is not read until either foo.txt and bar.txt
     * // has completely been read or baz.md has been read
     */
        mergeWithLimit<U>(this: Stream<Stream<U>>, number) : Stream<U>;

    /**
     * Indicates whether a stream has ended.
     */
        ended?: boolean
    }

}