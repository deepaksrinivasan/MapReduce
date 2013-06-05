'''Implements a relational join as a Map Reduce query. Mapper takes every record from a bag of records.
Reducer generates two buckets according to keys and performs the join'''


import MapReduce
import sys
import json

mr = MapReduce.MapReduce()
jenc = json.JSONEncoder(encoding='latin-1')


order_records = []
line_records = []
inputdata_list = []






'''def mapper(inputdatalist):
	for record in inputdatalist:
		key = record[0]
		#print(key,record)
		mr.emit_intermediate(key,record)'''


def mapper(record):
		mr.emit_intermediate(record[0],record)

def reducer(key,list_of_values):
	
	#print order_records, line_records
	for v in list_of_values:
		if key == "order":
			#print "order",1
			order_records.append(v)
			#emit_tuple = (key,order_records)
			#mr.emit(emit_tuple)	
		else:
			#print "line",1
			line_records.append(v)
			#emit_tuple = (key,line_records)
			#mr.emit(emit_tuple)	
	#if len(order_records)+len(line_records)== len(inputdata_list):
	for order_rec in order_records:
		for line_rec in line_records:
			if order_rec[1] == line_rec[1]:
				mr.emit((order_rec+line_rec))
	#print line_records




def executer(data,mapper,reducer):
	
	for line in data:
		record_list = []
		inputline = json.loads(line)
		for fields in inputline:
			record_list.append(fields)
		#print record_list
		mapper(record_list)
		inputdata_list.append(record_list)
	#mapper(inputdata_list)
	
	for key in mr.intermediate:
		reducer(key, mr.intermediate[key])
	#print inputdata_list
	
	
	for item in mr.result:
		#sum = sum+1
		#print (type(str(item[0])),type(item[1]))
		print jenc.encode(item)
		#print sum

		
	#print len(mapper_input),type(mapper_input)
	#print len(orderinputdata_list),len(lineinputdata_list)
	#print mapper_input[:][0]
	
	


		




def main():
	json_data = open(sys.argv[1],'r')
	executer(json_data, mapper, reducer)
	
if __name__ == '__main__':
	main()
	

