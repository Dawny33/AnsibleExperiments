def SWordCount(file_name):
	chars = words = lines = 0
	with open(file_name, 'r') as in_file:
	    for line in in_file:
	        lines += 1
	        words += len(line.split())
	        chars += len(line)

	return chars, words, lines

print SWordCount("text.txt")
