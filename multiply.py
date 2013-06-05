'''Sparse matrix multiplication 
The version according to Ullman et al.,'''


import MapReduce
import sys
import json

mr = MapReduce.MapReduce()
jenc = json.JSONEncoder(encoding='latin-1')


def mapper(record):
	matrix_id = record[0]
	row_id = record[1]
	col_id = record[2]
	if matrix_id == "a":
		for k in range(5):
			mr.emit_intermediate((row_id,k),(matrix_id,col_id,record[3]))
			#print dict_mat_a
	else:
		for k in range(5):
			mr.emit_intermediate((k,col_id),(matrix_id,row_id,record[3]))
		
	
			
	
	


def reducer(key,list_of_values):
	temp = 0
	list_a = []
	list_b = []
	for element in list_of_values:
		if element[0] == "a":
			list_a.append(element)
		else:
			list_b.append(element)
	for j in range(len(list_a)):
		for k in range(len(list_b)):
			if list_a[j][1] == list_b[k][1]:
				temp = temp + (list_a[j][2] * list_b[k][2])
	mr.emit((key[0],key[1],temp))
			
		
		#print key, v
		


def executer(data,mapper,reducer):
	#inputdata = []
	for line in data:
		inputdata = json.loads(line)
		mapper(inputdata)
	
	for key in mr.intermediate:
		#iter = iter+1
		#print  key, mr.intermediate[key]
		reducer(key, mr.intermediate[key])## SDS: Removed the first self argument
		
	for item in mr.result:
		#print (type(str(item[0])),type(item[1]))
		print jenc.encode(item)


def main():
	#print 'checking multiply.py'
	json_data = open(sys.argv[1],'r')
	executer(json_data, mapper, reducer)
	
if __name__ == '__main__':
	main()
