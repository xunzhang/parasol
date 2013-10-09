Loader in parasol
=================

Design
------
fmt is classified with patterns: linesplit, fmap, smap, fsmap, fvec.
linesplit reps simple lines reading while fmap, smap and fsmap reps sparse matrix, fvec reps dense matrix.
patterns and be coincide, simply used for general usage.

Usage
-----

-----------------------------------------------------------------------------
<table border="1">
    <tr>
        <th>textfile</th>
        <th>fmt</th>
        <th>pattern</th>
        <th>mix_flag</th>
        <th>parsername</th>
    </tr>
    <tr>
        <td>a.txt</td>
        <td>fset</td>
        <td>linesplit/fmap</td>
        <td>n/y</td>
        <td>parser_b('\t')</td>
    </tr>
    <tr>
        <td>a2.txt</td>
        <td>fset</td>
        <td>fmap</td>
        <td>y</td>
        <td>parser_c('\t', '|')</td>
    </tr>
    <tr>
        <td>b.txt</td>
        <td>bfs</td>
        <td>fmap</td>
        <td>n</td>
        <td>parser_b('\t')</td>
    </tr>
    <tr>
        <td>c.txt</td>
        <td>null</td>
        <td>fmap</td>
        <td>y</td>
        <td>parser_b('\t')</td>
    </tr>
    <tr>
        <td>d.txt</td>
        <td>fsv</td>
        <td>fsmap</td>
        <td>n</td>
        <td>parser_ussrt</td>
    </tr>
    <tr>
        <td>d2.txt</td>
        <td>fsv</td>
        <td>fsmap</td>
        <td>n</td>
        <td>parser_b('\t')</td>
    </tr>
    <tr>
        <td>e.txt</td>
        <td>fvec</td>
        <td>fvec</td>
        <td>y</td>
        <td>parser_d('\t')</td>
    </tr>
    <tr>
        <td>f.txt</td>
        <td>fs</td>
        <td>fmap</td>
        <td>n</td>
        <td>parser_c('\t', '|')</td>
    </tr>
    <tr>
        <td>g.txt</td>
        <td>fvec</td>
        <td>fvec</td>
        <td>n</td>
        <td>parser_e(' ', '|')</td>
    </tr>
    <tr>
        <td>g2.txt</td>
        <td>fvec</td>
        <td>fvec</td>
        <td>n</td>
        <td>parser_e(' ', '|')</td>
    </tr>
    <tr>
        <td>h.txt</td>
        <td>null</td>
        <td>fmap</td>
        <td>y</td>
        <td>parser_b('\t')</td>
    </tr>
</table>

--------------------------------------------------------------------------------

Note
----
-fmap in fmt column can be replaced by smap(follow input semantic)

-c.txt and h.txt is not defined by Changsheng(@https://svn.douban.com/projects/rivendell/wiki/apps)

-in fset and fs fmt, if sep1 == sep2 you must use parser_b instead of parser_c

-in fvec fmt, if sep1 == sep2, you must use parser_d instead of parser_e

-in fs fmt, sep between seconds must be different from sep in second and value

-you can use linesplit pattern in fset and fvec if you like(here we return a matrix)
