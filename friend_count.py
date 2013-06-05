'''Working copy of program to count the number of friends someone has. Uses the map reduce formalism
'''

import MapReduce
import sys
import json



mr = MapReduce.MapReduce()
jenc = json.JSONEncoder()

def mapper(record):
	mr.emit_intermediate(record[0],record[1])
	#print(record[0],record[1])


def reducer(key,list_of_values):
	num_friends = 0
	for v in list_of_values:
		num_friends = num_friends + 1
	mr.emit((key,num_friends))


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


