import json

f = open("Shakespeare.txt",'r')
content = f.read()
content = content.lower()
#print(content)

# split word
word_list = content.split()
print(len(word_list))

unique_word_dict = {}

for word in word_list:
    if word not in unique_word_dict.keys():
        unique_word_dict[word] = 1
    else:
        unique_word_dict[word] += 1

#print(unique_word_dict)

json.dump(unique_word_dict, open('unique_word_dict.json', 'wb'))
print('done...!')
