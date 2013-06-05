''''MapReduce instance to trim given dna strings and produce only unique trimmed DNA instances'''

import MapReduce
import sys
import json



mr = MapReduce.MapReduce()
jenc = json.JSONEncoder()

def mapper(record):
	seqid = record[0]
	seq = record[1]
	mr.emit_intermediate("key",seq[:-10])
	#print(seq,1)


def reducer(key, list_of_values):
	unique_trimmedlist = []
	#unique_trimmedlist.append(list_of_values[0])
	for v in list_of_values:
		if not v in unique_trimmedlist:
			unique_trimmedlist.append(v)
			#print len(unique_trimmedlist)
			mr.emit((v))
		


def executer(data, mapper, reducer):
        for line in data:
		inputline = json.loads(line)
		mapper_input = [inputline[0],inputline[1]]
		mapper(mapper_input)

        for key in mr.intermediate:
		#print len(mr.intermediate[key])
		reducer(key, mr.intermediate[key])## SDS: Removed the first self argument

        
        for item in mr.result:
			#print (type(str(item[0])),type(item[1]))
		print jenc.encode(item)
	#print type(mr.result), len(mr.result[0])



def main():
	json_data = open(sys.argv[1],'r')
	executer(json_data, mapper, reducer)
	
if __name__ == '__main__':
	main()


