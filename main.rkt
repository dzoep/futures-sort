#lang at-exp racket

(module+ test
  (require rackunit))

(require racket/future
         (for-syntax racket/syntax
                     syntax/parse)
         racket/require
         (filtered-in
          (λ (name)
            (and (or (regexp-match #rx"^unsafe-fx" name)
                     (regexp-match #rx"^unsafe-vector" name))
                 (regexp-replace #rx"unsafe-" name "")))
          racket/unsafe/ops)
         (only-in
          racket/fixnum
          fxvector?
          make-fxvector)
         scribble/srcdoc
         (for-doc scribble/manual
                  scribble-math/dollar
                  (for-label racket/unsafe/ops)))

(provide

 (parameter-doc
  futures-sort-parallel-depth
  (parameter/c exact-nonnegative-integer?)
  depth
  @{
    
    A parameter specifying the maximum depth of merge-sort where
    futures are leveraged. Total number of futures in the deepest layer is
    at most @${2^{depth}}.

    Default value is @${\log_2p} rounded up to whole integer for
    p=@racket[(processor-count)].

    })

 (proc-doc
  vector-futures-sort!
  (->i ((unsorted vector?))
       ((less-than? (any/c any/c . -> . any/c))
        (start (unsorted) (and/c exact-nonnegative-integer?
                                 (</c (vector-length unsorted))))
        (end (unsorted start) (and/c exact-nonnegative-integer?
                                     (>/c start)
                                     (<=/c (vector-length unsorted))))
        #:key (extract-key (any/c . -> . any/c))
        #:cache-keys? (cache-keys? boolean?))
       (res void?))
  ((λ (a b) (< a b))
   0
   (vector-length unsorted)
   (λ (x) x)
   #f
   )
  @{

    Sorts @racket[vector?] in place.

    The procedure uses merge sort with @${n} merge operations. Its
    overall algorithmic time complexity is @${O(n\cdot\log_2 n)} and
    memory complexity is @${O(n)} as it needs to allocate memory for copy
    of the original vector.

    The implementation uses futures for the first
    @racket[futures-sort-parallel-depth] splitting steps.

    If a custom compare function is provided, it should be a lambda
    term and not a reference to some other function. For example,
    providing @racket[fx<] as compare blocks running in parallel, but
    using the default compare function as is provides support for maximum
    parallelism.

    The @racket[#:cache-keys?] argument is provided only for
    compatibility with @racket[vector-sort!] and similar functions.

    })

 (proc-doc
  vector-futures-sort
  (->i ((unsorted vector?))
       ((less-than? (any/c any/c . -> . any/c))
        (start (unsorted) (and/c exact-nonnegative-integer?
                                 (</c (vector-length unsorted))))
        (end (unsorted start) (and/c exact-nonnegative-integer?
                                     (>/c start)
                                     (<=/c (vector-length unsorted))))
        #:key (extract-key (any/c . -> . any/c))
        #:cache-keys? (cache-keys? boolean?))
       (res vector?))
  ((λ (a b) (< a b))
   0
   (vector-length unsorted)
   (λ (x) x)
   #f
   )
  @{

    Sorts @racket[vector?] like @racket[vector-futures-sort!] without
    modifying the original unsorted vector and allocating a fresh vector
    for the result.

    })

 (proc-doc
  vector-futures-sort!/progress
  (->i ((unsorted vector?))
       ((less-than? (any/c any/c . -> . any/c))
        (start (unsorted) (and/c exact-nonnegative-integer?
                                 (</c (vector-length unsorted))))
        (end (unsorted start) (and/c exact-nonnegative-integer?
                                     (>/c start)
                                     (<=/c (vector-length unsorted))))
        #:key (extract-key (any/c . -> . any/c))
        #:cache-keys? (cache-keys? boolean?)
        #:progress-proc (progress-proc (or/c (fixnum? fixnum? . -> . any/c)
                                             false?))
        #:progress-sleep (progress-sleep positive?))
       (res void?))
  ((λ (a b) (< a b))
   0
   (vector-length unsorted)
   (λ (x) x)
   #f
   #f
   0.1
   )
  @{

    Like @racket[vector-futures-sort!].

    If @racket[progress-proc] is not #f, it gets called every
    @racket[progress-sleep] seconds. The procedure must accept two
    arguments:

    @racketblock[

                 (λ (progress total)
                   ...
                   )

                 ]

    The @racket[progress] is the number of merges already performed
    and total is @racket[(sub1 (- end start))].

    })

 (proc-doc
  vector-futures-sort/progress
  (->i ((unsorted vector?))
       ((less-than? (any/c any/c . -> . any/c))
        (start (unsorted) (and/c exact-nonnegative-integer?
                                 (</c (vector-length unsorted))))
        (end (unsorted start) (and/c exact-nonnegative-integer?
                                     (>/c start)
                                     (<=/c (vector-length unsorted))))
        #:key (extract-key (any/c . -> . any/c))
        #:cache-keys? (cache-keys? boolean?)
        #:progress-proc (progress-proc (or/c (fixnum? fixnum? . -> . any/c)
                                             false?))
        #:progress-sleep (progress-sleep positive?))
       (res void?))
  ((λ (a b) (< a b))
   0
   (vector-length unsorted)
   (λ (x) x)
   #f
   #f
   0.1
   )
  @{

    Sorts @racket[vector?] like @racket[vector-futures-sort!/progress]
    without modifying the original unsorted vector and allocating a fresh
    vector for the result.

    })

 (proc-doc
  fxvector-futures-sort!
  (->i ((unsorted fxvector?))
       ((less-than? (any/c any/c . -> . any/c))
        (start (unsorted) (and/c exact-nonnegative-integer?
                                 (</c (vector-length unsorted))))
        (end (unsorted start) (and/c exact-nonnegative-integer?
                                     (>/c start)
                                     (<=/c (vector-length unsorted))))
        #:key (extract-key (any/c . -> . any/c))
        #:cache-keys? (cache-keys? boolean?))
       (res void?))
  ((λ (a b) (unsafe-fx< a b))
   0
   (fxvector-length unsorted)
   (λ (x) x)
   #f
   )
  @{

    Like @racket[vector-futures-sort!] but for @racket[fxvector?].

    })

 (proc-doc
  fxvector-futures-sort
  (->i ((unsorted fxvector?))
       ((less-than? (any/c any/c . -> . any/c))
        (start (unsorted) (and/c exact-nonnegative-integer?
                                 (</c (vector-length unsorted))))
        (end (unsorted start) (and/c exact-nonnegative-integer?
                                     (>/c start)
                                     (<=/c (vector-length unsorted))))
        #:key (extract-key (any/c . -> . any/c))
        #:cache-keys? (cache-keys? boolean?))
       (res fxvector?))
  ((λ (a b) (< a b))
   0
   (fxvector-length unsorted)
   (λ (x) x)
   #f
   )
  @{

    Sorts @racket[fxvector?] like @racket[fxvector-futures-sort!]
    without modifying the original unsorted fxvector and allocating a
    fresh fxvector for the result.

    })

 (proc-doc
  fxvector-futures-sort!/progress
  (->i ((unsorted fxvector?))
       ((less-than? (any/c any/c . -> . any/c))
        (start (unsorted) (and/c exact-nonnegative-integer?
                                 (</c (vector-length unsorted))))
        (end (unsorted start) (and/c exact-nonnegative-integer?
                                     (>/c start)
                                     (<=/c (vector-length unsorted))))
        #:key (extract-key (any/c . -> . any/c))
        #:cache-keys? (cache-keys? boolean?)
        #:progress-proc (progress-proc (or/c (fixnum? fixnum? . -> . any/c)
                                             false?))
        #:progress-sleep (progress-sleep positive?))
       (res void?))
  ((λ (a b) (unsafe-fx< a b))
   0
   (fxvector-length unsorted)
   (λ (x) x)
   #f
   #f
   0.1
   )
  @{

    Like @racket[vector-futures-sort!/progress] but for @racket[fxvector?].

    })

 (proc-doc
  fxvector-futures-sort/progress
  (->i ((unsorted fxvector?))
       ((less-than? (any/c any/c . -> . any/c))
        (start (unsorted) (and/c exact-nonnegative-integer?
                                 (</c (vector-length unsorted))))
        (end (unsorted start) (and/c exact-nonnegative-integer?
                                     (>/c start)
                                     (<=/c (vector-length unsorted))))
        #:key (extract-key (any/c . -> . any/c))
        #:cache-keys? (cache-keys? boolean?)
        #:progress-proc (progress-proc (or/c (fixnum? fixnum? . -> . any/c)
                                             false?))
        #:progress-sleep (progress-sleep positive?))
       (res fxvector?))
  ((λ (a b) (unsafe-fx< a b))
   0
   (fxvector-length unsorted)
   (λ (x) x)
   #f
   #f
   0.1
   )
  @{

    Sorts @racket[fxvector?] like
    @racket[fxvector-futures-sort!/progress] without modifying the
    original unsorted fxvector and allocating a fresh fxvector for the
    result.

    })

 (proc-doc
  futures-sort
  (->i ((lst list?)
        (less-than? (any/c any/c . -> . any/c)))
       (#:key (extract-key (any/c . -> . any/c))
        #:cache-keys? (cache-keys? boolean?))
       (res list?))
  ((λ (x) x)
   #f
   )
  @{

    A simple wrapper that converts @racket[lst] to @racket[vector?],
    sorts it using @racket[vector-futures-sort!] and converts it back to
    @racket[list?].

    })

 )

(define futures-sort-parallel-depth
  (make-parameter
   (inexact->exact (ceiling (log (processor-count) 2)))))

(define-syntax (generate-type-futures-sorts stx)
  (syntax-parse stx
    ((_
      (~optional (~seq (~and #:progress progress-kw)))
      (~optional (~seq (~and #:create create-kw)))
      type
      default-compare)
     (with-syntax* ((gen-progress (attribute progress-kw))
                    (gen-create (attribute create-kw))
                    (proc-name (datum->syntax
                                stx
                                (string->symbol
                                 (string-append
                                  (symbol->string (syntax->datum #'type))
                                  "-futures-sort"
                                  (if (syntax->datum #'gen-create)
                                      ""
                                      "!")
                                  (if (syntax->datum #'gen-progress)
                                      "/progress"
                                      "")
                                  ))
                                ))
                    (type-set! (datum->syntax
                                stx
                                (string->symbol
                                 (string-append
                                  (symbol->string (syntax->datum #'type))
                                  "-set!"))))
                    (type-ref (datum->syntax
                               stx
                               (string->symbol
                                (string-append
                                 (symbol->string (syntax->datum #'type))
                                 "-ref"))))
                    (type-length (datum->syntax
                                  stx
                                  (string->symbol
                                   (string-append
                                    (symbol->string (syntax->datum #'type))
                                    "-length"))))
                    (make-type (datum->syntax
                                stx
                                (string->symbol
                                 (string-append
                                  "make-"
                                  (symbol->string (syntax->datum #'type))))))
                    )
       #`(define (proc-name
                  unsorted
                  (compare default-compare)
                  (start 0)
                  (end #f)
                  #:key (key (λ (x) x))
                  #:cache-keys? (cache-keys? #f)
                  #,@(if (syntax->datum #'gen-progress)
                         #'(#:progress-proc (progress-proc #f)
                            #:progress-sleep (progress-sleep 0.1))
                         '())
                  )
           ; Get the length and create scratchpad
           (define unsorted-length (type-length unsorted))
           (define scratchpad (make-type unsorted-length))

           ; Alias or create result
           (define result
             #,(if (syntax->datum #'gen-create)
                   #'(make-type unsorted-length)
                   #'unsorted))

           ; Calculate total number of futures with current parallelism setting
           (define futures-depth (futures-sort-parallel-depth))
           (define num-futures (expt 2 futures-depth))

           ; Optional progress handling
           #,@(if (syntax->datum #'gen-progress)
                  #'(
                     (define total-merges (fx- unsorted-length 1))

                     ; Create progresses vector, start progress thread and create stop
                     ; procedure
                     (define (create-progresses+stop)
                       (define progresses (make-fxvector num-futures))
                       (define progress-running #t)
                       (define progress-thread
                         (thread
                          (λ ()
                            (let loop ()
                              (when progress-running
                                (define progress
                                  (let loop ((i 0)
                                             (acc 0))
                                    (if (fx< i num-futures)
                                        (loop (fx+ i 1)
                                              (fx+ acc (fxvector-ref progresses i)))
                                        acc)))
                                (progress-proc progress total-merges)
                                (sleep progress-sleep)
                                (loop))))))

                       ; Stops the loop and waits for the thread
                       (define (progress-stop)
                         (set! progress-running #f)
                         (thread-wait progress-thread)
                         (progress-proc total-merges total-merges))

                       ; Must be assigned at once
                       (values progresses progress-stop))

                     ; Create progresses vector and stop proc if required
                     (define-values
                       (progresses progress-stop)
                       (if progress-proc
                           (create-progresses+stop)
                           (values #f #f)))
                     )
                  '())

           ; Generic merge stage
           (define (merge! from1 to1/from2 to2 depth fid)
             #,(when (syntax->datum #'gen-progress)
                 #'(when progress-proc
                     (fxvector-set! progresses fid (fx+ (fxvector-ref progresses fid) 1))))
             (define src (if (fx= (fxand depth 1) 1) result scratchpad))
             (define dst (if (fx= (fxand depth 1) 1) scratchpad result))
             (let loop1 ((i from1)
                         (j to1/from2)
                         (k from1))
               ; Always select correct element based on compare function
               (cond
                 ((and (fx< i to1/from2)
                       (fx< j to2))
                  (define v1 (type-ref src i))
                  (define v2 (type-ref src j))
                  (cond
                    ((compare (key v1)
                              (key v2))
                     (type-set! dst k v1)
                     (loop1 (fx+ i 1) j (fx+ k 1)))
                    (else
                     (type-set! dst k v2)
                     (loop1 i (fx+ j 1) (fx+ k 1)))))
                 (else
                  (let loop2 ((i i)
                              (k k))
                    ; Finish from the first part
                    (cond
                      ((fx< i to1/from2)
                       (type-set! dst k (type-ref src i))
                       (loop2 (fx+ i 1) (fx+ k 1)))
                      (else
                       (let loop3 ((j j)
                                   (k k))
                         ; Finish from the second part
                         (when (fx< j to2)
                           (type-set! dst k (type-ref src j))
                           (loop3 (fx+ j 1) (fx+ k 1)))))))))))

           ; Splitting part
           (define (sort-step from to (depth 0) (fid 0))
             (define cnt (fx- to from))
             (cond ((fx> cnt 3)
                    ; >2 means we do a proper split/merge
                    ; this is the only part to leverage futures
                    (define cnt2 (fxrshift cnt 1))
                    (define to1/from2 (fx+ from cnt2))
                    (define to2 to)
                    (cond ((fx< depth futures-depth)
                           (let ((f1 (future
                                      (λ ()
                                        (sort-step from to1/from2
                                                   (fx+ depth 1)
                                                   fid)
                                        (void)))))
                             (sort-step to1/from2 to2
                                        (fx+ depth 1)
                                        (fxior fid (fxlshift 1 depth)))
                             (touch f1)))
                          (else
                           (sort-step from to1/from2 (fx+ depth 1) fid)
                           (sort-step to1/from2 to2 (fx+ depth 1) fid)
                           ))
                    (merge! from to1/from2 to2 depth fid))
                   ((fx= cnt 3)
                    ; Unrolled 3-elements sort
                    (define v1 (type-ref unsorted from))
                    (define v2 (type-ref unsorted (fx+ from 1)))
                    (define v3 (type-ref unsorted (fx+ from 2)))
                    (define dst (if (fx= (fxand depth 1) 1) scratchpad result))
                    (define k1 (key v1))
                    (define k2 (key v2))
                    (define k3 (key v3))
                    (define-values (r1 r2 r3)
                      (if (compare k1 k2)
                          (if (compare k1 k3)
                              (if (compare k2 k3)
                                  (values v1 v2 v3)
                                  (values v1 v3 v2))
                              (values v3 v1 v2))
                          (if (compare k2 k3)
                              (if (compare k1 k3)
                                  (values v2 v1 v3)
                                  (values v2 v3 v1))
                              (values v3 v2 v1))))
                    (type-set! dst from r1)
                    (type-set! dst (fx+ from 1) r2)
                    (type-set! dst (fx+ from 2) r3))
                   ((fx= cnt 2)
                    #,(when (syntax->datum #'gen-progress)
                        #'(when progress-proc
                            (fxvector-set! progresses fid
                                           (fx+ (fxvector-ref progresses fid) 1))))
                    ; =2 - swap in-place or from the other
                    (define v1 (type-ref unsorted from))
                    (define v2 (type-ref unsorted (fx+ from 1)))
                    (define dst (if (fx= (fxand depth 1) 1) scratchpad result))
                    (define v1first (compare (key v1)
                                             (key v2)))
                    (type-set! dst
                               from
                               (if v1first v1 v2))
                    (type-set! dst
                               (fx+ from 1)
                               (if v1first v2 v1)))
                   ((fx= cnt 1)
                    #,(if (syntax->datum #'gen-create)
                          ; =1 - always copy
                          #'(type-set! (if (fx= (fxand depth 1) 1)
                                           scratchpad
                                           result)
                                       from
                                       (type-ref unsorted from))
                          ; =1 - only copy if it must go to scratchpad
                          #'(when (fx= (fxand depth 1) 1)
                              (type-set! scratchpad from (type-ref unsorted from)))))
                   ))

           ; Start
           (sort-step start
                      (or end unsorted-length))

           ; Optional progress handling
           #,(when (syntax->datum #'gen-progress)
               ; Stop progress thread
               #'(when progress-proc
                   (progress-stop)))

           #,(if (syntax->datum #'gen-create)
                 ; Return the newly created (fx)vector
                 #'result
                 ; Return void - as sort-vector! does
                 #'(void)))))))

; fxvector-futures-sort!
(generate-type-futures-sorts fxvector (λ (a b) (fx< a b)))

; vector-futures-sort!
(generate-type-futures-sorts vector (λ (a b) (< a b)))

; fxvector-futures-sort!/progress
(generate-type-futures-sorts #:progress fxvector (λ (a b) (fx< a b)))

; vector-futures-sort!/progress
(generate-type-futures-sorts #:progress vector (λ (a b) (< a b)))

; fxvector-futures-sort
(generate-type-futures-sorts #:create fxvector (λ (a b) (fx< a b)))

; vector-futures-sort
(generate-type-futures-sorts #:create vector (λ (a b) (< a b)))

; fxvector-futures-sort/progress
(generate-type-futures-sorts #:progress #:create fxvector (λ (a b) (fx< a b)))

; vector-futures-sort/progress
(generate-type-futures-sorts #:progress #:create vector (λ (a b) (< a b)))

(define (futures-sort lst
                      less-than?
                      #:key (extract-key (λ (x) x))
                      #:cache-keys? (cache-keys? #f))
  (define vec (list->vector lst))
  (vector-futures-sort! vec less-than? #:key extract-key)
  (vector->list vec))

(module+ test

  (define test-random-seed 4)  ; chosen by fair dice roll.
  (define test-sample-size 100)
  (define test-max-number 1000000)
  

  (random-seed test-random-seed)
  (define random-vector (for/vector ((i test-sample-size)) (random test-max-number)))
  (define sorted-vector (vector-sort random-vector <))

  (let ((random-vector-copy (vector-copy random-vector)))
    (vector-futures-sort! random-vector-copy)
    (check-equal? random-vector-copy sorted-vector
                  "vector-futures-sort!"))

  (let ((random-vector-copy (vector-copy random-vector)))
    (check-equal? (vector-futures-sort random-vector-copy) sorted-vector
                  "vector-futures-sort result")
    (check-equal? random-vector-copy random-vector
                  "vector-futures-sort argument"))

  (let ((random-vector-copy (vector-copy random-vector)))
    (define last-current #f)
    (define last-total #f)
    (vector-futures-sort!/progress random-vector-copy
                                   #:progress-proc (λ (current total)
                                                     (set! last-current current)
                                                     (set! last-total total)))
    (check-equal? random-vector-copy sorted-vector
                  "vector-futures-sort!/progress result")
    (check-not-equal? last-current #f
                      "vector-futures-sort!/progress progress-proc current value")
    (check-equal? last-total (sub1 (vector-length random-vector))
                  "vector-futures-sort!/progress progress-proc total value"))

  (let ((random-vector-copy (vector-copy random-vector)))
    (define last-current #f)
    (define last-total #f)
    (define result
      (vector-futures-sort/progress random-vector-copy
                                    #:progress-proc (λ (current total)
                                                      (set! last-current current)
                                                      (set! last-total total))))
    (check-equal? result sorted-vector
                  "vector-futures-sort/progress result")
    (check-equal? random-vector-copy random-vector
                  "vector-futures-sort/progress argument")
    (check-not-equal? last-current #f
                      "vector-futures-sort/progress progress-proc current value")
    (check-equal? last-total (sub1 (vector-length random-vector))
                  "vector-futures-sort/progress progress-proc total value"))


  (define random-fxvector (make-fxvector test-sample-size))
  (for ((i test-sample-size))
    (fxvector-set! random-fxvector i
                   (vector-ref random-vector i)))
  (define sorted-fxvector (make-fxvector test-sample-size))
  (for ((i test-sample-size))
    (fxvector-set! sorted-fxvector i
                   (vector-ref sorted-vector i)))


  (define (fxvector-copy fxv)
    (define fxv-length (fxvector-length fxv))
    (define result (make-fxvector fxv-length))
    (for ((i fxv-length))
      (fxvector-set! result i
                     (fxvector-ref fxv i)))
    result)
  

  (let ((random-fxvector-copy (fxvector-copy random-fxvector)))
    (fxvector-futures-sort! random-fxvector-copy)
    (check-equal? random-fxvector-copy sorted-fxvector
                  "fxvector-futures-sort!"))

  (let ((random-fxvector-copy (fxvector-copy random-fxvector)))
    (check-equal? (fxvector-futures-sort random-fxvector-copy) sorted-fxvector
                  "fxvector-futures-sort result")
    (check-equal? random-fxvector-copy random-fxvector
                  "fxvector-futures-sort argument"))

  (let ((random-fxvector-copy (fxvector-copy random-fxvector)))
    (define last-current #f)
    (define last-total #f)
    (fxvector-futures-sort!/progress random-fxvector-copy
                                     #:progress-proc (λ (current total)
                                                       (set! last-current current)
                                                       (set! last-total total)))
    (check-equal? random-fxvector-copy sorted-fxvector
                  "fxvector-futures-sort!/progress result")
    (check-not-equal? last-current #f
                      "fxvector-futures-sort!/progress progress-proc current value")
    (check-equal? last-total (sub1 (fxvector-length random-fxvector))
                  "fxvector-futures-sort!/progress progress-proc total value"))

  (let ((random-fxvector-copy (fxvector-copy random-fxvector)))
    (define last-current #f)
    (define last-total #f)
    (define result
      (fxvector-futures-sort/progress random-fxvector-copy
                                      #:progress-proc (λ (current total)
                                                        (set! last-current current)
                                                        (set! last-total total))))
    (check-equal? result sorted-fxvector
                  "fxvector-futures-sort/progress result")
    (check-equal? random-fxvector-copy random-fxvector
                  "fxvector-futures-sort/progress argument")
    (check-not-equal? last-current #f
                      "fxvector-futures-sort/progress progress-proc current value")
    (check-equal? last-total (sub1 (fxvector-length random-fxvector))
                  "fxvector-futures-sort/progress progress-proc total value"))
  
  )
