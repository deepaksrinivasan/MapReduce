'''Sparse matrix multiplication 
Passing a row/column directly into the mapper'''



import MapReduce
import sys
import json

mr = MapReduce.MapReduce()
jenc = json.JSONEncoder(encoding='latin-1')


def mapper(records):
	dict_mat_a = {}
	dict_mat_b = {}
	for record in records:
		matrix_id = record[0]
		row_id = record[1]
		col_id = record[2]
		if matrix_id == "a":
			dict_mat_a.setdefault(row_id, [])
			dict_mat_a[row_id].append((col_id,record[3]))
			#print dict_mat_a
		else:
			dict_mat_b.setdefault(col_id, [])
			dict_mat_b[col_id].append((row_id,record[3]))
			#print dict_mat_b
	for a_key in dict_mat_a.keys():
		for b_key in dict_mat_b.keys():
			#print ((a_key,b_key),[dict_mat_a[a_key],dict_mat_b[b_key]])
			mr.emit_intermediate((a_key,b_key),(dict_mat_a[a_key],dict_mat_b[b_key]))
	
		
	
			
	
	


def reducer(key,list_of_values):
	temp = 0
	list_a = list_of_values[0][0]
	list_b = list_of_values[0][1]
	for j in range(len(list_a)):
		for k in range(len(list_b)):
			if list_a[j][0] == list_b[k][0]:
				temp = temp + (list_a[j][1] * list_b[k][1])
	mr.emit((key[0],key[1],temp))
			
		
		#print key, v
		


def executer(data,mapper,reducer):
	inputdata = []
	for line in data:
		inputdata.append(json.loads(line))
	mapper(inputdata)
	
	for key in mr.intermediate:
		#iter = iter+1
		#print  key, mr.intermediate[key]
		reducer(key, mr.intermediate[key])## SDS: Removed the first self argument
		
	for item in mr.result:
		#print (type(str(item[0])),type(item[1]))
		print jenc.encode(item)


def main():
	json_data = open(sys.argv[1],'r')
	executer(json_data, mapper, reducer)
	
if __name__ == '__main__':
	main()
