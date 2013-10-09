Loader in parasol
=================

Design
------
fmt is classified with patterns: linesplit, fmap, smap, fsmap, fvec.
linesplit reps simple lines reading while fmap, smap and fsmap reps sparse matrix, fvec reps dense matrix.
patterns and be coincide, simply used for general usage.

Usage
-----
textfile	fmt	pattern	mix_flag	parsername
a.txt	fset	linesplit/fmap	no/mix	parser_b('\t')
a2.txt	fset	fmap	mix	parser_c('\t', '|')
b.txt	bfs	fmap	n	parser_b('\t')
c.txt	null	fmap	y	parser_b('\t')	
d.txt	fsv	fsmap	n	parser_ussrt
d2.txt	fsv	fsmap	n	parser_b('\t')
e.txt	fvec	fvec	y	parser_d('\t')
f.txt	fs	fmap	n	parser_c('\t', '|')
g.txt	fvec	fvec	n	parser_e(' ', '|')
g2.txt	fvec	fvec	n	parser_e(' ', '|')
h.txt	null	fmap	y	parser_b('\t')

Note
----
-fmap in fmt column can be replaced by smap(follow input semantic)

-c.txt and h.txt is not defined by Changsheng(@https://svn.douban.com/projects/rivendell/wiki/apps)

-in fset and fs fmt, if sep1 == sep2 you must use parser_b instead of parser_c

-in fvec fmt, if sep1 == sep2, you must use parser_d instead of parser_e

-in fs fmt, sep between seconds must be different from sep in second and value

-you can use linesplit pattern in fset and fvec if you like(here we return a matrix)
