#! /usr/bin/python

parser_a = lambda sep : lambda line : [int(l) for l in line.strip('\n').split(sep)]
parser_b = lambda sep : lambda line : [l for l in line.strip('\n').split(sep)]

# example: /mfs/alg/dbsync/book_interest/000.csv
# example: d.txt
# user_id	subject_id	status	rating	time
def parser_ussrt(line, sep = '\t'):
    stf = []
    l = line.strip('\n').split(sep)
    if l[2] == 'P':
	if l[3] == 'NULL' or l[3] == '':
	    # 3.7 is avg rating
	    stf = [int(l[0]), int(l[1]), 3.7]
	else:
	    stf = [int(l[0]), int(l[1]), int(l[3])]
    return stf
