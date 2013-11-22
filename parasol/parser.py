# example: 1001 1 2 4 - parser_b(' ')
parser_a = lambda sep : lambda line : [int(l) for l in line.strip('\n').split(sep)]

parser_d = lambda sep : lambda line : [line.strip('\n').split(sep)[0]] + [float(l) for l in line.strip('\n').split(sep)[1:]]
parser_e = lambda sep1, sep2 : lambda line : [line.strip('\n').split(sep1)[0]] + [float(l) for l in line.strip('\n').split(sep1)[1].split(sep2)]

# example: a\tb\tc\td\n - parser_b('\t') - a.txt
# example: a b c d\n - parser_b(' ')
# example a|b|c|d\n - parser_b('|')
# example: a\tb\n
#           a\tc\n - parser_b('\t') - b.txt
# example: a\tb\n
#          a\tc\td\n - parser_b('\t') - c.txt
parser_b = lambda sep : lambda line : line.strip('\n').split(sep)

# example: a\tb|c|d\n
#          b\ta|d\n - parser_c('\t', '|') - a2.txt
# example: a\tb:0.1|c:0.2|d:0.4 - parser_c('\t', '|') - f.txt
# example: a\tb:0.1\n
#          a\tc:0.2\td:0.4\n - parser_c('\t', '\t') - h.txt
parser_c = lambda sep1, sep2 : lambda line : [line.strip('\n').split(sep1)[0]] + line.strip('\n').split(sep1)[1].split(sep2)

# example: d.txt, /mfs/alg/dbsync/book_interest/000.csv
# user_id\tsubject_id\tstatus\trating\ttime\n
def parser_ussrt(line, sep = '\t'):
    stf = []
    l = line.strip('\n').split(sep)
    if l[2] == 'P':
	if l[3] == 'NULL' or l[3] == '':
	    # 3.7 is avg rating
	    stf = [int(l[0]), int(l[1]), 3.7]
	else:
	    stf = [int(l[0]), int(l[1]), float(l[3])]
    return stf
