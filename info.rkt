#lang info
(define collection "futures-sort")
(define deps '("base" "scribble-lib"))
(define build-deps '("scribble-lib" "racket-doc" "rackunit-lib" "scribble-math" "at-exp-lib"))
(define scribblings '(("scribblings/futures-sort.scrbl" ())))
(define pkg-desc "Parallel merge-sort for vector? and fxvector? using futures.")
(define version "0.2")
(define pkg-authors '(Dominik Pantůček))
