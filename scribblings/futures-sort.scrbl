#lang scribble/manual
@require[@for-label[futures-sort
                    racket/base
		    racket/fixnum
		    racket/future]
	 scribble/extract
	 scribble-math]

@title[#:style (with-html5 manual-doc-style)]{@racketmodname[futures-sort]}

@author[
@author+email["Dominik Pantůček" "dominik.pantucek@trustica.cz"]]

@defmodule[futures-sort]

This library leverages futures for implementing parallel merge-sort of
@racket[vector?] and @racket[fxvector?]. By default it tries to use all
available processors as reported by @racket[(processor-count)].

It is possible to use this library to speed up already written programs using
stock @racket[vector-sort], @racket[vector-sort!], and @racket[sort] by
renaming the required "futures" functions.

@racketblock[
(require (rename-in futures-sort
                    [vector-futures-sort vector-sort]
		    [vector-futures-sort! vector-sort!]
		    [futures-sort sort]))
]

All the functions provided support the same positional and keyword arguments as
their respective counterparts and are meant to be a parallel drop-in
replacement for those. The @racket[#:cache-keys?] argument is unused and
is accepted only to provide compatibility.

@include-extracted["../main.rkt"]
