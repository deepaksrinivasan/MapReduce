import MapReduce
import sys
import json



mr = MapReduce.MapReduce()
jenc = json.JSONEncoder()

def mapper(record):
	mr.emit_intermediate(record[0],record[1])
	#print(record[0],record[1])
	
def reducer(key,list_of_values):
	#print (key,list_of_values)
	mr.emit((key,list_of_values))
	
def executer(data,mapper,reducer):
	for line in data:
		inputline = json.loads(line)
		mapper_input = [inputline[0],inputline[1]]
		#print inputline
		mapper(inputline)
		
	for key in mr.intermediate:
		reducer(key,mr.intermediate[key])
		
	for item in mr.result:
		item_key = jenc.encode(item[0])
		item_val = item[1]
		#print item_key,item_val
		for friend in item_val:
			#print [item_key,jenc.encode(friend)]
			#print [jenc.encode(friend),item_key]
			if not friend in mr.intermediate:
				print [item_key,jenc.encode(friend)]
				print [jenc.encode(friend),item_key]
			else:
				if not item_key in mr.intermediate[friend]:
					print [item_key,jenc.encode(friend)]
					print [jenc.encode(friend),item_key]
			
		

def main():
	json_data = open(sys.argv[1],'r')
	executer(json_data,mapper,reducer)
	
if __name__ == '__main__':
	main()

