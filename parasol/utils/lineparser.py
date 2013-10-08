#! /usr/bin/python

# user_id	subject_id	rating
def parser_a(line):
    return [int(l) for l in line.strip('\n').split('\t')]

# normal string stuff
# example: b.txt
def parser_b(line):
    return [l for l in line.strip('\n').split('\t')]

# example: /mfs/alg/dbsync/book_interest/000.csv
# example: d.txt
# user_id	subject_id	status	rating	time
def parser_ussrt(line):
    stf = []
    l = line.strip('\n').split('\t')
    if l[2] == 'P':
	if l[3] == 'NULL' or l[3] == '':
	    # 3.7 is avg rating
	    stf = [int(l[0]), int(l[1]), 3.7]
	else:
	    stf = [int(l[0]), int(l[1]), int(l[3])]
    return stf
