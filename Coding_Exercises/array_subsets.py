
"""
This function assumes there are no repeated numbers in the array
"""

def subsetA(arr):
	# try:
	# 	assert len(arr) != len(set(arr))
	# 	print("There are repeated list elements in the input array")

	# except Exception:

	input_list = arr
	arr_len = len(arr)
	print("Len of arr: ", arr_len)

	# Is the number of elements in arr even or odd
	if arr_len % 2 == 0:
		max_A_len = int((arr_len / 2) - 1)
		print("Len of arr is even, so max_A_len: ", max_A_len)
	else:
		max_A_len = int((arr_len - 1) / 2)
		print("Len of arr is odd, so max_A_len: ", max_A_len)

	# determine the elements in A_arr
	cnt = 1
	while cnt <= max_A_len:
		A_arr = []
		B_arr = []

		if cnt == 1:
			for num in arr:
				A_arr = [i for i in arr if i is num]
				B_arr = [i for i in arr if i is not num]
				#print(A_arr)
				#print(B_arr)
				if sum(A_arr) > sum(B_arr):
					print("A_array: ", A_arr)
					print("B_array: ", B_arr)					
					print("A and B union sorted: ", sorted(A_arr+B_arr))
					print("A and B union sorted == Input array: ", sorted(A_arr+B_arr) == sorted(arr))
					break
				# else:
				# 	print("B is greater or equal to A. Try again!")
				if num == arr[-1]:
					print("The length of the input array is too small")
		else:
			# get all possible combination with len of cnt
			i_z_list = [chr(x) for x in range(ord('i'), ord('z')+1)]
			#print(i_z_list)
			#print([char+char for char in i_z_list])

			code_str_list = []
			for_loop_str_list = []
			comb_list_str = "input_list[]"
			for_loop_str = "for [] in range(len(input_list))"
			for i in range(cnt):
			#for i in range(len(input_list)):

				if i <= cnt:
					it = i_z_list[i]
				else:
					rep = i // len(i_z_list)
					disp = i % len(i_z_list)
					it = i_z_list[disp]*rep

				code_str_list.append(comb_list_str.replace("[]","["+ it +"]"))
				for_loop_replace = for_loop_str.replace("[]", it)
				if i != 0:
					for_loop_replace = for_loop_replace.replace("range(len","range(i+" + str(i)+ ", len")
				for_loop_str_list.append(for_loop_replace)

			code_str = ", ".join(code_str_list)
			code_str = "[" + code_str + "]"
			for_loop_code_str = " ".join(for_loop_str_list)
			full_code_str = "[" + code_str + " " + for_loop_code_str + "]"

			#print(full_code_str)
			#print(eval("arr"))
			all_combinations = eval(full_code_str)
			#print(all_combinations)

			# Find A_arr
			for array in all_combinations:
				#print("A_array: ", array)
				#print("B_array: ", [num for num in input_list if num not in array])
				if sum(array) > sum([num for num in input_list if num not in array]):
					A_arr = array
					B_arr = [num for num in input_list if num not in array]
					# print("A_array: ", A_arr)
					# print("B_array: ", B_arr)
					# print("A and B union sorted: ", sorted(A_arr+B_arr))
					# print("A and B union sorted == Input array: ", sorted(A_arr+B_arr) == sorted(arr))
					cnt = max_A_len + 1
					break

		cnt+=1
	return A_arr


input_list = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]
A_array = subsetA(input_list)
print(A_array)
#all_combinations = [[input_list[i], input_list[j], input_list[k]] for i in range(len(input_list)) for j in range(i+1, len(input_list)) for k in range(i+2, len(input_list))]
#print(all_combinations)

# x = 2
# assert x == 2

# print(16//3)
# print(16%3)
# print('i'*3)