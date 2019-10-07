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

@include-extracted["../main.rkt"]
