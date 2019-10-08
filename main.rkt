#lang at-exp racket

(module+ test
  (require rackunit))

(require racket/future
         (for-syntax racket/syntax
                     syntax/parse)
         racket/require
         (filtered-in
          (λ (name) (regexp-replace #rx"unsafe-" name ""))
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
       ((compare procedure?))
       (res void?))
  ((λ (a b) (< a b)))
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

    })

 (proc-doc
  vector-futures-sort!/progress
  (->i ((unsorted vector?))
       ((compare procedure?)
        #:progress-proc (progress-proc (or/c procedure? false?))
        #:progress-sleep (progress-sleep positive?))
       (res void?))
  ((λ (a b) (< a b)) #f 0.1)
  @{

    Like @racket[vector-futures-sort!].

    If @racket[progress-proc] is not #f, it gets called every
    @racket[progress-sleep] seconds.

    })

 (proc-doc
  fxvector-futures-sort!
  (->i ((unsorted fxvector?))
       ((compare procedure?))
       (res void?))
  ((λ (a b) (unsafe-fx< a b)))
  @{

    Like @racket[vector-futures-sort!] but for @racket[fxvector?].

    })

 (proc-doc
  fxvector-futures-sort!/progress
  (->i ((unsorted fxvector?))
       ((compare procedure?)
        #:progress-proc (progress-proc (or/c procedure? false?))
        #:progress-sleep (progress-sleep positive?))
       (res void?))
  ((λ (a b) (unsafe-fx< a b)) #f 0.1)
  @{

    Like @racket[vector-futures-sort!/progress] but for @racket[fxvector?].

    })
 )

(define futures-sort-parallel-depth
  (make-parameter
   (inexact->exact (ceiling (log (processor-count) 2)))))

(define-syntax (generate-type-futures-sorts stx)
  (syntax-parse stx
    ((_
      (~optional (~seq (~and #:progress progress-kw)))
      type
      default-compare)
     (with-syntax* ((gen-progress (attribute progress-kw))
                    (proc-name (datum->syntax
                                stx
                                (string->symbol
                                 (string-append
                                  (symbol->string (syntax->datum #'type))
                                  "-futures-sort!"
                                  (if (syntax->datum #'gen-progress)
                                      "/progress"
                                      "")
                                  ))
                                )))
       #`(define (proc-name
                  unsorted
                  (compare default-compare)
                  #,@(if (syntax->datum #'gen-progress)
                         #'(#:progress-proc (progress-proc #f)
                            #:progress-sleep (progress-sleep 0.1))
                         '())
                  )
           ; Check arguments
           (when (not (#,(datum->syntax
                          stx
                          (string->symbol
                           (string-append
                            (symbol->string (syntax->datum #'type))
                            "?")))
                       unsorted))
             (error (string-append "Argument `unsorted' must be "
                                   (symbol->string (quote type)))))
           
           ; Get and check the length and create scratchpad
           (define unsorted-length (#,(datum->syntax
                                       stx
                                       (string->symbol
                                        (string-append
                                         (symbol->string (syntax->datum #'type))
                                         "-length")))
                                    unsorted))
           (when (not (fixnum? unsorted-length))
             (error (string-append "Argument `unsorted' is too long!")))
           (define scratchpad (#,(datum->syntax
                                  stx
                                  (string->symbol
                                   (string-append
                                    "make-"
                                    (symbol->string (syntax->datum #'type)))))
                               unsorted-length))

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

           ; Alias accessors
           (define type-set! #,(datum->syntax
                                stx
                                (string->symbol
                                 (string-append
                                  (symbol->string (syntax->datum #'type))
                                  "-set!"))))
           (define type-ref #,(datum->syntax
                               stx
                               (string->symbol
                                (string-append
                                 (symbol->string (syntax->datum #'type))
                                 "-ref"))))

           ; Generic merge stage
           (define (merge! from1 to1/from2 to2 depth fid)
             #,(when (syntax->datum #'gen-progress)
                 #'(when progress-proc
                     (fxvector-set! progresses fid (fx+ (fxvector-ref progresses fid) 1))))
             (define src (if (fx= (fxand depth 1) 1) unsorted scratchpad))
             (define dst (if (fx= (fxand depth 1) 1) scratchpad unsorted))
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
                    ((compare v1 v2)
                     (type-set! dst k v1)
                     (loop1 (fx+ i 1) j (fx+ k 1)))
                    (else
                     (type-set! dst k v2)
                     (loop1 i (fx+ j 1) (fx+ k 1)))))
                 (else
                  (let loop2 ((i i)
                              (j j)
                              (k k))
                    ; Finish from the first part
                    (cond
                      ((fx< i to1/from2)
                       (type-set! dst k (type-ref src i))
                       (loop2 (fx+ i 1) j (fx+ k 1)))
                      (else
                       (let loop3 ((i i)
                                   (j j)
                                   (k k))
                         ; Finish from the second part
                         (when (fx< j to2)
                           (type-set! dst k (type-ref src j))
                           (loop3 i (fx+ j 1) (fx+ k 1)))))))))))

           ; Splitting part
           (define (sort-step from to (depth 0) (fid 0))
             (define cnt (fx- to from))
             (cond ((fx> cnt 2)
                    ; >2 means we do a proper split/merge
                    ; this is the only part to leverage futures
                    (define cnt2 (fxrshift cnt 1))
                    (define from1 from)
                    (define to1/from2 (fx+ from cnt2))
                    (define to2 to)
                    (cond ((fx< depth futures-depth)
                           (let ((f1 (future
                                      (λ ()
                                        (sort-step from1 to1/from2
                                                   (fx+ depth 1)
                                                   fid)
                                        #f)))
                                 (f2 (future
                                      (λ ()
                                        (sort-step to1/from2 to2
                                                   (fx+ depth 1)
                                                   (fxior fid (fxlshift 1 depth)))
                                        #f))))
                             (or (touch f1)
                                 (touch f2))))
                          (else
                           (sort-step from1 to1/from2 (fx+ depth 1) fid)
                           (sort-step to1/from2 to2 (fx+ depth 1) fid)
                           ))
                    (merge! from1 to1/from2 to2 depth fid))
                   ((fx= cnt 2)
                    #,(when (syntax->datum #'gen-progress)
                        #'(when progress-proc
                            (fxvector-set! progresses fid
                                           (fx+ (fxvector-ref progresses fid) 1))))
                    ; =2 - swap in-place or from the other
                    (define v1 (type-ref unsorted from))
                    (define v2 (type-ref unsorted (fx+ from 1)))
                    (define dst (if (fx= (fxand depth 1) 1) scratchpad unsorted))
                    (define v1first (compare v1 v2))
                    (type-set! dst
                               from
                               (if v1first v1 v2))
                    (type-set! dst
                               (fx+ from 1)
                               (if v1first v2 v1)))
                   ((fx= cnt 1)
                    ; =1 - only copy if it must go to scratchpad
                    (when (fx= (fxand depth 1) 1)
                      (type-set! scratchpad from (type-ref unsorted from))))
                   ))

           ; Start
           (sort-step 0 unsorted-length)

           ; Optional progress handling
           #,(when (syntax->datum #'gen-progress)
               ; Stop progress thread
               #'(when progress-proc
                   (progress-stop)))

           ; Return void - as sort-vector! does
           (void))))))

; fxvector-futures-sort!
(generate-type-futures-sorts fxvector (λ (a b) (fx< a b)))

; vector-futures-sort!
(generate-type-futures-sorts vector (λ (a b) (< a b)))

; fxvector-futures-sort!/progress
(generate-type-futures-sorts #:progress fxvector (λ (a b) (fx< a b)))

; vector-futures-sort!/progress
(generate-type-futures-sorts #:progress vector (λ (a b) (< a b)))

(module+ test
  ;; Any code in this `test` submodule runs when this file is run using DrRacket
  ;; or with `raco test`. The code here does not run when this file is
  ;; required by another module.

  (check-equal? (+ 2 2) 4))
