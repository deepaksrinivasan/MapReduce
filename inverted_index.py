'''Copy to generate inverted index of a documentb corpus. Generates a list for every token occuring in the
document corpus along with the docid list where the token occurs'''

import MapReduce
import sys
import json



mr = MapReduce.MapReduce()
jenc = json.JSONEncoder(encoding='latin-1')



def mapper(record):
	docid = record[0]
	words = record[1]
	for w in words.split():
		mr.emit_intermediate(w,docid)
	



def reducer(key, list_of_values):
	docidlist = []
	for v in list_of_values:
		docidlist.append(str(v))
	emit_tuple = (key,list(set(docidlist)))
	mr.emit(emit_tuple)		



def executer(data, mapper, reducer):
        for line in data:
			inputline = json.loads(line)
			docid = inputline[0]
			words = inputline[1]
			mapper_input = [docid,words]
			mapper(mapper_input)

        for key in mr.intermediate:
            reducer(key, mr.intermediate[key])## SDS: Removed the first self argument

        
        for item in mr.result:
			#print (type(str(item[0])),type(item[1]))
            print jenc.encode(item)



def main():
	
	inputdata = []
	json_data = open(sys.argv[1],'r')
	executer(json_data, mapper, reducer)
	
if __name__ == '__main__':
	main()


