lst = [1,2,3,4,5,6,7,8,9,10]

# revrese_array =[]
# print(len(arr))
mid_lst = len(lst)
print("mid of the list :", mid_lst//2)

def reverse_arr(lst):
    print("Before the reverse",lst)
    n = len(lst)
    # print("N value : ",n)
    for i in range(n//2):
        temp = lst[i]
        lst[i] = lst[n-i-1]
        lst[n - i - 1] = temp
    print("After the reverse",lst)



reverse_arr(lst)